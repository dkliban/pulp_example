import os
from collections import namedtuple
from gettext import gettext as _
from logging import getLogger
from urllib.parse import urlparse, urlunparse

from django.core.files import File
from django.db.utils import IntegrityError
from django.db import models
from django.db import transaction
from django.db.models import Q

from pulpcore.plugin.models import (Artifact, Content, ContentArtifact, DeferredArtifact, Importer,
    ProgressBar, Publisher, RepositoryContent)

from .downloaders import ContentUnitDownloader

log = getLogger(__name__)

BUFFER_SIZE = 65536

# Changes needed.
Delta = namedtuple('Delta', ('additions', 'removals'))
# Natural key.
FileTuple = namedtuple('Key', ('path', 'digest'))

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
            key = FileTuple(path=content.path, digest=content.digest)
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
        download = self.get_download(self.feed_url, os.path.basename(parsed_url.path))
        download()
        self.path = download.writer.path
        remote = set()
        for entry in self.read():
            key = FileTuple(path=entry['path'], digest=entry['digest'])
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

        Args:
            delta (namedtuple)
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

        # Find content that is already in Pulp
        fields = {f.name for f in ExampleContent.natural_key_fields}

        q = Q()
        for c in delta.additions:
            q |= Q(path=c.path, digest=c.digest)

        # Find all content being added that already exists in Pulp and has all of it's Artifacts
        # downloaded. Associate with the repository.
        ready_to_associate = ExampleContent.objects.filter(q).filter(contentartifact__artifact__isnull=False)
        with ProgressBar(message="Associating units already in Pulp",
                         total=ready_to_associate.count()) as bar:
            for content in ready_to_associate:
                association = RepositoryContent(
                    repository=self.repository,
                    content=content)
                association.save()
                bar.increment()
                # Remove it from the delta
                key = FileTuple(path=content.path, digest=content.digest)
                delta.additions.remove(key)

        # Find all content being added that already exists in Pulp but needs 1+ Artifacts to be
        # downloaded.
        content_missing_artifacts = ExampleContent.objects.filter(q).filter(contentartifact__artifact=None)


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
        self.content_dict = {}  # keys are unit keys and values are lists of deferred artifacts
        # associated with the content
        description = _("Dowloading artifacts and adding content to the repository.")
        # Start reporting progress
        progress_bar = ProgressBar(message=description, total=len(delta.additions))
        progress_bar.save()
        downloader = ContentUnitDownloader(self.next_content_unit(delta.additions))

        with progress_bar:
            for id, downloaded_files in downloader:
                content = self.content_dict.pop(id)
                self._create_and_associate_content(content, downloaded_files)
                progress_bar.increment()
                log.warning('content_unit = {0}'.format(content))

    def next_content_unit(self, additions):
        """
        Generator of ExampleContent, ContentArtifacts, and DeferredArtifacts.

        This generator is responsible for creating all the models needed to create ExampleContent in
        Pulp. It stores the ExampleContent in a dictionary to be used after all the related
        Artifacts have been downloaded. This generator emits a dictionary with two keys: id and
        deferred_artifacts. The id is the key that was used to store ExampleContent in
        self.contetn_dict and the deferred_artifacts is a list of DeferredArtifacts that can be
        used to download Artifacts for ExampleContent.
        """
        parsed_url = urlparse(self.feed_url)
        root_dir = os.path.dirname(parsed_url.path)

        for content in additions:
            path = os.path.join(root_dir, content.path)
            url = urlunparse(parsed_url._replace(path=path))
            example_content = ExampleContent(path=content.path, digest=content.digest)
            content_id = example_content.natural_key()
            self.content_dict[content_id] = example_content
            content_artifact = ContentArtifact(content=example_content, relative_path=content.path)
            deferred_artifacts = [DeferredArtifact(url=url, importer=self, sha256=content.digest,
                                                   content_artifact=content_artifact)]
            yield {'id': content_id, 'deferred_artifacts': deferred_artifacts}


    def _create_and_associate_content(self, content, deferred_artifacts):
        """
        Saves ExampleContent and all related models to the database

        This method saves ExampleContent, ContentArtifacts, DeferredArtifacts and Artifacts to
        the database inside a single transaction.

        Args:
            content (:class:`pulp_example.app.models.ExampleContent`): An instance of
                ExampleContent to be saved to the database.
            deferred_artifacts (dict): A dictionary where keys are instances of
                :class:`pulpcore.plugin.models.DeferredArtifact` and values are dictionaries that
                contain information about files downloaded using the DeferredArtifacts.
        """

        # Save Artifacts, ContentArtifacts, DeferredArtifacts, and Content in a transaction
        with transaction.atomic():
            # Save content
            try:
                with transaction.atomic():
                    content.save()
                    log.warning("Created content")
            except IntegrityError:
                key = {f.name: getattr(content, f.name) for f in
                       content.natural_key_fields}
                content = type(content).objects.get(**key)
            # Add content to the repository
            association = RepositoryContent(
                repository=self.repository,
                content=content)
            association.save()
            log.warning("Created association with repository")

            for deferred_artifact, file_info in deferred_artifacts.items():
                filename = file_info.pop('filename')
                with File(open(filename, mode='rb')) as file:
                    try:
                        with transaction.atomic():
                            artifact = Artifact(file=file, size=file.size, **file_info)
                            artifact.save()
                    except IntegrityError:
                        artifact = Artifact.objects.get(sha256=file_info['sha256'])

                content_artifact = deferred_artifact.content_artifact
                content_artifact.artifact = artifact
                try:
                    with transaction.atomic():
                        content_artifact.save()
                except IntegrityError:
                    content_artifact = ContentArtifact.objects.get(
                        content=content_artifact.content,
                        relative_path=content_artifact.relative_path)
                deferred_artifact.content_artifact = content_artifact
                deferred_artifact.artifact = artifact
                try:
                    with transaction.atomic():
                        deferred_artifact.save()
                except IntegrityError:
                    pass


