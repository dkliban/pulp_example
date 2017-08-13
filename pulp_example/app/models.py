import os
from collections import namedtuple
from gettext import gettext as _
from logging import getLogger
from urllib.parse import urlparse, urlunparse

from django.db import models

from pulpcore.plugin.models import (Artifact, Content, ContentArtifact, DeferredArtifact, Importer,
    ProgressBar, Publisher, RepositoryContent)

from .downloaders import ContentUnitDownloader, DownloadAll

log = getLogger(__name__)

BUFFER_SIZE = 65536

# Changes needed.
Delta = namedtuple('Delta', ('additions', 'removals'))
# Natural key.
FileTuple = namedtuple('Key', ('path', 'digest', 'size'))

Line = namedtuple('Line', ('number', 'content'))


class ExampleContent(Content):
    """
    The "example" content type.

    Content of this type represents a collection of 1 or more files uniquely
    identified by path and SHA256 digest.

    Fields:
        path (str): The file relative path.
        digest (str): The SHA256 HEX digest.

    """
    TYPE = 'example'

    path = models.TextField(blank=False, null=False)
    digest = models.TextField(blank=False, null=False)

    natural_key_fields = (path, digest)

    class Meta:
        unique_together = (
            'path',
            'digest'
        )


class ExamplePublisher(Publisher):
    """
    Publisher for "file" content.
    """
    TYPE = 'file'

    def publish(self):
        """
        Perform a publish.

        Publish behavior for the file plugin has not yet been implemented.
        """
        raise NotImplementedError


class ExampleImporter(Importer):
    """
    Importer for "example" content.

    This importer uses the plugin API to download content serially and import it into the
    repository. Any content removed from the remote repository since the previous sync is also
    removed from the Pulp repository.
    """
    TYPE = 'example'

    def _fetch_inventory(self):
        """
        Fetch existing content in the repository.

        Returns:
            set: of Key.
        """
        inventory = set()
        q_set = ExampleContent.objects.filter(repositories=self.repository)
        q_set = q_set.only(*[f.name for f in ExampleContent.natural_key_fields])
        for content in (c.cast() for c in q_set):
            key = FileTuple(path=content.path, digest=content.digest,
                            size=content.contentartifact_set.all()[0].artifact.size)
            inventory.add(key)
        return inventory

    @staticmethod
    def parse(line):
        """
        Parse the specified line from the manifest into an Entry.

        Args:
            line (Line): A line from the manifest.

        Returns:
            Entry: An entry.

        Raises:
            ValueError: on parsing error.
        """
        part = [s.strip() for s in line.content.split(',')]
        if len(part) != 3:
            raise ValueError(
                _('Error: manifest line:{n}: '
                  'must be: <path>, <digest>, <size>').format(
                    n=line.number))
        return {'path': part[0], 'digest': part[1], 'size': int(part[2])}

    def read(self):
        """
        Read the file at `path` and yield entries.

        Yields:
            Entry: for each line.
        """
        with open(self.path) as fp:
            n = 0
            for line in fp.readlines():
                n += 1
                line = line.strip()
                if not line:
                    continue
                if line.startswith('#'):
                    continue
                yield self.parse(Line(number=n, content=line))

    def _find_delta(self, mirror=True):
        """
        Using the manifest and set of existing (natural) keys,
        determine the set of content to be added and deleted from the
        repository.  Expressed in natural key.
        Args:
            inventory (set): Set of existing content (natural) keys.
            mirror (bool): Faked mirror option.
                TODO: should be replaced with something standard.

        Returns:
            Delta: The needed changes.
        """
        inventory = self._fetch_inventory()
        parsed_url = urlparse(self.feed_url)
        # Start asyncio event loop


        parsed_url = urlparse(self.feed_url)
        download = self.get_download(self.feed_url, os.path.basename(parsed_url.path))
        download()
        self.path = download.writer.path
        remote = set()
        for entry in self.read():
            key = FileTuple(path=entry['path'], digest=entry['digest'], size=entry['size'])
            remote.add(key)
        additions = remote - inventory
        if mirror:
            removals = inventory - remote
        else:
            removals = set()
        return Delta(additions=additions, removals=removals)

    def deferred_sync(self, delta):
        """
        Synchronize the repository with the remote repository without downloading artifacts.
        """
        description = _("Adding file content to the repository without downloading artifacts.")
        progress_bar = ProgressBar(message=description, total=len(delta.additions))

        parsed_url = urlparse(self.feed_url)
        root_dir = os.path.dirname(parsed_url.path)

        with progress_bar:
            for entry in delta.additions:
                path = os.path.join(root_dir, entry.path)
                url = urlunparse(parsed_url._replace(path=path))
                content, content_created = ExampleContent.objects.get_or_create(path=entry.path,
                                                                             digest=entry.digest)
                # Add content to the repository
                association = RepositoryContent(
                    repository=self.repository,
                    content=content)
                association.save()
                # ContentArtifact is required for all download policies. It is used for publishing.
                content_artifact, content_artifact_created = ContentArtifact.objects.get_or_create(
                    content=content, relative_path=entry.path)
                # DeferredArtifact is required for deferred download policies.
                deferred_artifact, deferred_artifact_created = \
                    DeferredArtifact.objects.get_or_create(
                    url=url, importer=self, content_artifact=content_artifact, sha256=entry.digest)

                try:
                    artifact = Artifact.objects.get(sha256=entry.digest)
                    content_artifact.artifact = artifact
                except Artifact.DoesNotExist:
                    pass
                content_artifact.save()
                # Report progress
                progress_bar.increment()



    def sync(self):
        """
        Synchronize the repository with the remote repository.
        """
        delta = self._find_delta()
        if self.download_policy != self.IMMEDIATE:
            self.deferred_sync(delta)
        else:
            self.full_sync(delta)

        # Remove content if there is any to remove
        if delta.removals:
            # Build a query that uniquely identifies all content that needs to be removed.
            q = models.Q()
            for key in delta.removals:
                q |= models.Q(examplecontent__path=key.path, examplecontent__digest=key.digest)
            q_set = self.repository.content.filter(q)
            RepositoryContent.objects.filter(
                repository=self.repository).filter(content=q_set).delete()

    def full_sync(self, delta):
        """
        Synchronize the repository with the remote repository without downloading artifacts.
        """
        description = _("Dowloading artifacts and adding content to the repository.")
        # Start reporting progress
        progress_bar = ProgressBar(message=description, total=len(delta.additions))
        progress_bar.save()

        download_all = DownloadAll(self)

        parsed_url = urlparse(self.feed_url)
        root_dir = os.path.dirname(parsed_url.path)

        for content in delta.additions:
            path = os.path.join(root_dir, content.path)
            url = urlunparse(parsed_url._replace(path=path))
            content_unit = ExampleContent(path=content.path, digest=content.digest)
            deferred_artifacts = {content.path: DeferredArtifact(url=url, importer=self,
                                                      sha256=content.digest)}
            downloader = ContentUnitDownloader(content_unit, deferred_artifacts)
            download_all.register_for_downloading(downloader)
        with progress_bar:
            for content_unit in download_all:
                progress_bar.increment()
                log.warning('content_unit = {0}'.format(content_unit))


    @staticmethod
    def get_checksums(file):
        """
        Calculates all checksums for a file.

        Args:
            file (:class:`django.core.files.File`): open file handle

        Returns:
            dict: Dictionary where keys are checksum names and values are checksum values
        """
        hashers = {}
        for algorithm in hashlib.algorithms_guaranteed:
            hashers[algorithm] = getattr(hashlib, algorithm)()
        while True:
            data = file.read(BUFFER_SIZE)
            if not data:
                break
            for algorithm, hasher in hashers.items():
                hasher.update(data)
        ret = {}
        for algorithm, hasher in hashers.items():
            ret[algorithm] = hasher.hexdigest()
        return ret
