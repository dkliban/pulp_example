import aiohttp
import asyncio
import async_timeout
import hashlib
import os

from collections import defaultdict
from logging import getLogger

from django.core.files import File
from django.db.utils import IntegrityError
from django.db import transaction

from pulpcore.plugin.models import (Artifact, Content, ContentArtifact, DeferredArtifact, Importer,
    ProgressBar, RepositoryContent)


log = getLogger(__name__)


async def ConcurrentHttpDownloader(url, timeout=10):
    async with aiohttp.ClientSession() as session:
        algorithms = {n: hashlib.new(n) for n in Artifact.DIGEST_FIELDS}
        attributes = {}
        with async_timeout.timeout(timeout):
            async with session.get(url) as response:
                filename = os.path.basename(url)
                with open(filename, 'wb') as f_handle:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            print('Finished downloading {filename}'.format(filename=filename))
                            break
                        f_handle.write(chunk)
                        for algorithm in algorithms.values():
                            algorithm.update(chunk)
                        # push each chunk through the digest and size validators
            await response.release()
            attributes.update({n: a.hexdigest() for n, a in algorithms.items()})
            attributes['filename'] = filename
            return url, attributes


class DownloadAll(object):

    def __init__(self, importer):
        self.importer = importer
        self.downloads_not_done = []
        self.content_units_not_done = []
        self.urls = defaultdict(list) # dict of lists keyed on urls which point to a list of ContentUnitDownloader objects
        self.loop = asyncio.get_event_loop()
        self.artifact_metadata = defaultdict(dict) # dict of dicts keyed on urls which point to a
        #  dict of attributes for creating an artifcat

    def register_for_downloading(self, content_unit):
        self.content_units_not_done.append(content_unit)
        for url in content_unit.urls:
            if len(self.urls[url]) == 0:
                # This is the first time we've seen this url so make a downloader
                downloader_for_url = ConcurrentHttpDownloader(url)
                self.downloads_not_done.append(downloader_for_url)
            self.urls[url].append(content_unit)

    def _find_and_remove_done_content_unit(self):
        for index, content_unit in enumerate(self.content_units_not_done):
            if content_unit.done:
                self.content_units_not_done.pop(index)
                return content_unit

    def _create_content(self, content_unit):
        # Save Artifacts, ContentArtifacts, DeferredArtifacts, and Content in a transaction
        with transaction.atomic():
            # Save content
            try:
                with transaction.atomic():
                    content_unit.content.save()
                    log.warning("Created content")
            except IntegrityError:
                key = {f.name: getattr(content_unit.content, f.name) for f in
                       content_unit.content.natural_key_fields}
                content_unit.content = type(content_unit.content).objects.get(**key)
            # Add content to the repository
            association = RepositoryContent(
                repository=self.importer.repository,
                content=content_unit.content)
            association.save()
            log.warning("Created association with repository")

            for path, da in content_unit.deferred_artifacts.items():
                url = da.url
                artifact_attrs = self.artifact_metadata[url]
                filename = artifact_attrs.pop('filename')
                with File(open(filename, mode='rb')) as file:
                    try:
                        with transaction.atomic():
                            artifact = Artifact(file=file, size=file.size, **artifact_attrs)
                            artifact.save()
                    except IntegrityError:
                        artifact = Artifact.objects.get(sha256=artifact_attrs['sha256'])

                content_artifact = content_unit.content_artifacts[path]
                content_artifact.artifact = artifact
                try:
                    with transaction.atomic():
                        content_artifact.save()
                except IntegrityError:
                    content_artifact = ContentArtifact.objects.get(
                        content=content_artifact.content, relative_path=path)
                da.content_artifact = content_artifact
                da.artifact = artifact
                try:
                    with transaction.atomic():
                        da.save()
                except IntegrityError:
                    pass

    def __iter__(self):
        return self

    def __next__(self):
        while self.downloads_not_done:
            done_this_time, self.downloads_not_done = self.loop.run_until_complete(asyncio.wait(self.downloads_not_done, return_when=asyncio.FIRST_COMPLETED))
            for task in done_this_time:
                # TODO check for errors here in task.exception() or similar
                url, attributes = task.result()
                self.artifact_metadata[url] = attributes
                for content_unit in self.urls[url]:
                    content_unit.finished_urls.append(url)
                content_unit = self._find_and_remove_done_content_unit()
                if content_unit:
                    self._create_content(content_unit)
                    return content_unit
        content_unit = self._find_and_remove_done_content_unit()
        if content_unit:
            self._create_content(content_unit)
            return content_unit
        else:
            raise StopIteration()


class ContentUnitDownloader(object):

    def __init__(self, content, deferred_artifacts):
        """
        Args:
            content (:class:`pulpcore.plugin.models.Content`): An instance of Content that will be
                saved after all the artifacts have been downloaded and created in the database.
            deferred_artifacts (dict): Keys are relative paths and values are instances of
                :class:`pulpcore.plugin.models.DeferredArtifact` that will be saved once
                Artifacts are downloaded from URLs referenced in the DeferredArtifact.
        """
        self.content = content
        self.content_artifacts = {}
        self.deferred_artifacts = deferred_artifacts
        urls = []
        # Create ContentArtifacts and get a list of URLs
        #import pydevd
        #pydevd.settrace('localhost', port=3012, stdoutToServer=True, stderrToServer=True)
        for path, deferred_artifact in deferred_artifacts.items():
            self.content_artifacts[path] = ContentArtifact(content=content, relative_path=path)
            urls.append(deferred_artifact.url)
        self.urls = set(urls)
        self.finished_urls = []

    @property
    def done(self):
        return len(self.urls) == len(self.finished_urls)

