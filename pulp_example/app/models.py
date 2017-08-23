import asyncio
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

from pulpcore.plugin.download import GroupDownloader


log = getLogger(__name__)

BUFFER_SIZE = 65536

# Changes needed.
Delta = namedtuple('Delta', ('additions', 'removals'))
# Natural key.
Key = namedtuple('Key', ('path', 'digest'))

Line = namedtuple('Line', ('number', 'content'))


class ExampleContentManager(models.Manager):
    """
    A custom manager for all models that inherit from Content.

    The ContentManager can be used to find existing content in Pulp. This is useful when trying
    to determine whether content needs to be downloaded or not.
    """
    def find_by_unit_key(self, unit_keys, partial=False):
        """
        Returns a queryset built from the unit keys.

        Args:
            unit_keys (Iterable): An iterable of dictionaries where each key is a member of
                Content._meta.unique_together.
            partial (bool): Designates whether or not Content with missing artifacts should be
                included in the QuerySet.

        Returns:
            QuerySet of Content that corresponds to the unit keys passed in. When 'partial' is
                True, the QuerySet includes Content that is missing Artifacts.
        """
        q = models.Q()
        for key in unit_keys:
            unit_key_dict = {}
            for field in ExampleContent.unit_key_fields():
                unit_key_dict[field] = getattr(key, field)
            q |= models.Q(**unit_key_dict)
        if partial:
            return super().get_queryset().filter(q)
        else:
            return super().get_queryset().filter(q).filter(
                contentartifact__artifact__isnull=False)

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
    objects = ExampleContentManager()

    class Meta:
        unique_together = (
            'path',
            'digest'
        )

    @classmethod
    def unit_key_fields(cls):
        for unique in cls._meta.unique_together:
            for field in unique:
                yield field


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
        if self.download_policy == self.IMMEDIATE:
            q_set = q_set.filter(contentartifact__artifact__isnull=False)
        q_set = q_set.only(*[field for field in ExampleContent.unit_key_fields()])
        for content in (c.cast() for c in q_set):
            key = Key(path=content.path, digest=content.digest)
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

    def read_manifest(self):
        """
        Read the file at `path` and yield entries.

        Yields:
            Entry: for each line.
        """
        with open(self.manifest_path) as fp:
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
        download = self.get_downloader(self.feed_url)
        loop = asyncio.get_event_loop()
        done_this_time, downloads_not_done = loop.run_until_complete(asyncio.wait([download]))
        for task in done_this_time:
            download_result = task.result()
            self.manifest_path = download_result.path
        remote = set()
        for entry in self.read_manifest():
            key = Key(path=entry['path'], digest=entry['digest'])
            remote.add(key)
        additions = remote - inventory
        if mirror:
            removals = inventory - remote
        else:
            removals = set()
        return Delta(additions=additions, removals=removals)

    def sync(self):
        """
        Synchronize the repository with the remote repository.
        """
        self.content_dict = {}  # keys are unit keys and values are lists of deferred artifacts
        # associated with the content
        delta = self._find_delta()

        # Find all content being added that already exists in Pulp and associate with repository.
        fields = {f for f in ExampleContent.unit_key_fields()}
        if self.download_policy == self.IMMEDIATE:
            # Filter out any content that still needs to have artifacts downloaded
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions).only(*fields)
        else:
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions,
                                                                         partial=True
                                                                         ).only(*fields)
        added = self.associate_existing_content(ready_to_associate)
        remaining_additions = delta.additions - added
        delta = Delta(additions=remaining_additions, removals=delta.removals)

        if self.download_policy != self.IMMEDIATE:
            self.deferred_sync(delta)
        else:
            self.full_sync(delta)

        # Remove content if there is any to remove
        if delta.removals:
            # Build a query that uniquely identifies all content that needs to be removed.
            q = models.Q()
            for key in delta.removals:
                q |= models.Q(examplecontent__path=key['path'],
                              examplecontent__digest=key['digest'])
            q_set = self.repository.content.filter(q)
            RepositoryContent.objects.filter(
                repository=self.repository).filter(content=q_set).delete()

    def associate_existing_content(self, content_q):
        """
        Associates existing content to the importer's repository
        
        Args:
            content_q (queryset): Queryset that will return content that needs to be associated
                with the importer's repository.

        Returns:
            Set of natural keys representing each piece of content associated with the repository.
        """
        added = set()
        with ProgressBar(message="Associating units already in Pulp with the repository",
                         total=content_q.count()) as bar:
            for content in content_q:
                association = RepositoryContent(
                    repository=self.repository,
                    content=content)
                association.save()
                bar.increment()
                # Remove it from the delta
                key = Key(path=content.path, digest=content.digest)
                added.add(key)
        return added

    def deferred_sync(self, delta):
        """
        Synchronize the repository with the remote repository without downloading artifacts.

        Args:
            delta (namedtuple)
        """
        description = _("Adding file content to the repository without downloading artifacts.")
        progress_bar = ProgressBar(message=description, total=len(delta.additions))

        with progress_bar:
            for item in self.next_content_unit(delta.additions):
                content = self.content_dict[item['id']]
                deferred_artifacts = {}
                for deferred_artifact in item['deferred_artifacts']:
                    deferred_artifacts[deferred_artifact] = None
                self._create_and_associate_content(content, deferred_artifacts)
                progress_bar.increment()

    def full_sync(self, delta):
        """
        Synchronize the repository with the remote repository without downloading artifacts.
        """
        description = _("Dowloading artifacts and adding content to the repository.")
        downloader = GroupDownloader(self)
        downloader.schedule_from_iterator(self.next_content_unit(delta.additions))

        with ProgressBar(message=description, total=len(delta.additions)) as bar:
            for group_id, downloaded_dict in downloader:
                content = self.content_dict.pop(group_id)
                self._create_and_associate_content(content, downloaded_dict)
                bar.increment()
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
        for entry in self.read_manifest():
            key = Key(path=entry['path'], digest=entry['digest'])
            if key in additions:
                path = os.path.join(root_dir, entry['path'])
                url = urlunparse(parsed_url._replace(path=path))
                example_content = ExampleContent(path=entry['path'], digest=entry['digest'])
                content_id = tuple(getattr(example_content, f) for f in example_content.unit_key_fields())
                self.content_dict[content_id] = example_content
                # The content is set on the content_artifact right before writing to the
                # database. This helps deal with race conditions when saving Content.
                content_artifact = ContentArtifact(relative_path=entry['path'])
                deferred_artifacts = [DeferredArtifact(url=url,
                                                       importer=self,
                                                       sha256=entry['digest'],
                                                       size=entry['size'],
                                                       content_artifact=content_artifact)]
                yield {'id': content_id, 'deferred_artifacts': deferred_artifacts}

    def _create_and_associate_content(self, content, group_result):
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
                key = {f: getattr(content, f) for f in
                       content.unit_key_fields()}
                content = type(content).objects.get(**key)
            # Add content to the repository
            association = RepositoryContent(
                repository=self.repository,
                content=content)
            association.save()
            log.warning("Created association with repository")

            for deferred_artifact, download_result in group_result.items():
                if download_result:
                    # Create artifact that was downloaded and deal with race condition
                    with File(open(download_result.path, mode='rb')) as file:
                        try:
                            with transaction.atomic():
                                artifact = Artifact(file=file,
                                                    **download_result.artifact_attributes)
                                artifact.save()
                        except IntegrityError:
                            artifact = Artifact.objects.get(sha256=download_result.artifact_attributes['sha256'])
                else:
                    # Try to find an artifact if one already exists
                    try:
                        with transaction.atomic():
                            # try to find an artifact from information in deferred artifact
                            artifact = Artifact.objects.get(sha256=deferred_artifact.sha256)
                    except Artifact.DoesNotExist:
                        artifact = None
                content_artifact = deferred_artifact.content_artifact
                content_artifact.artifact = artifact
                content_artifact.content = content
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


