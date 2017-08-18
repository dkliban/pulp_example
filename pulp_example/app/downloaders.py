import aiohttp
import asyncio
import async_timeout
import hashlib
import os

from collections import defaultdict
from logging import getLogger

from pulpcore.download.http import ConcurrentHttpDownloader
from pulpcore.plugin.download import Factory

log = getLogger(__name__)


class ContentUnitDownloader(object):

    def __init__(self, content_to_download, importer):
        """
        Args:
            content_to_download (iterable): An iterable of tuples (id, [DeferredArtifact])
        """
        self.importer = importer
        self.content_to_download = content_to_download
        self.downloads_not_done = set()
        self.content_units_not_done = []
        self.urls = defaultdict(list) # dict of lists keyed on urls which point to a list of ContentUnitDownloader objects
        self.loop = asyncio.get_event_loop()
        self.first_run = True
        #  dict of attributes for creating an artifcat

    def schedule_download(self, id, deferred_artifacts):
        """
        Schedules deferred_artifacts to be downloaded

        Args:
            deferred_artifacts (list): A list of :class:`pulpcore.plugin.models.DeferredArtifact`
                instances that have not been saved to the database.
        """
        content_unit = ContentToDownload(id, deferred_artifacts)
        self.content_units_not_done.append(content_unit)
        for url in content_unit.urls:
            if len(self.urls[url]) == 0:
                # This is the first time we've seen this url so make a downloader

                downloader_for_url = Factory(self.importer).build(url, None)
                self.downloads_not_done.add(downloader_for_url)
            self.urls[url].append(content_unit)

    def _find_and_remove_done_content_unit(self):
        for index, content_unit in enumerate(self.content_units_not_done):
            if content_unit.done:
                self.content_units_not_done.pop(index)
                return content_unit


    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns:
            tuple: The first element is a content identifier and the second element is a
                dictionary where the keys are :class:`pulpcore.plugin.models.DeferredArtifact` and
                the values are dictionaries containing information about the downloaded file. The
                keys of this dictionary are 'filename', 'size', 'sha1' , 'md5', 'sha224',
                'sha256', 'sha384', and 'sha512'.
        """
        if self.first_run:
            # Download no more than 20 pieces of Content at a time. This may equal 20 or more
            # Artifacts.
            for i in range(20):
                try:
                    self.schedule_download(**next(self.content_to_download))
                except StopIteration:
                    pass
            self.first_run = False
        while self.downloads_not_done:
            done_this_time, self.downloads_not_done = \
                self.loop.run_until_complete(asyncio.wait(self.downloads_not_done,
                                                          return_when=asyncio.FIRST_COMPLETED))
            try:
                self.schedule_download(**next(self.content_to_download))
            except StopIteration:
                pass
            for task in done_this_time:
                # TODO check for errors here in task.exception() or similar
                url, attributes = task.result()
                for content_unit in self.urls[url]:
                    content_unit.file_downloaded(url, attributes)
                content_unit = self._find_and_remove_done_content_unit()
                if content_unit:
                    return content_unit.finished_downloads
        content_unit = self._find_and_remove_done_content_unit()
        if content_unit:
            return content_unit.finished_downloads
        else:
            raise StopIteration()


class ContentToDownload(object):

    def __init__(self, id, deferred_artifacts):
        """
        Args:
            deferred_artifacts (list): A list of :class:`pulpcore.plugin.models.DeferredArtifact`
                instances that have not been saved to the database.
        """
        self.id = id
        self.deferred_artifacts = {}
        self.downloaded_files = {}
        urls = []
        for deferred_artifact in deferred_artifacts:
            self.deferred_artifacts[deferred_artifact.url] = deferred_artifact
            urls.append(deferred_artifact.url)
        self.urls = set(urls)
        self.finished_urls = []

    def file_downloaded(self, url, file_attributes):
        """
        Update the ContentUnit with file attributes calculated during the download from the URL

        Args:
            url (string): the URL which was used to download the file
            file_attributes (dict): A dictionary where the keys are 'filename', 'size', 'md5',
                'sha1', 'sha224', 'sha256', 'sha384', and 'sha512'
        """
        self.finished_urls.append(url)
        self.downloaded_files[url] = file_attributes

    @property
    def done(self):
        return len(self.urls) == len(self.finished_urls)

    @property
    def finished_downloads(self):
        ret = {}
        for url, deferred_artifact in self.deferred_artifacts.items():
            ret[deferred_artifact] = self.downloaded_files[url]
        return self.id, ret