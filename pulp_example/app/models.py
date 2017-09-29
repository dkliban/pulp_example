import asyncio
import os
from collections import namedtuple
from gettext import gettext as _
from logging import getLogger
from urllib.parse import urlparse, urlunparse

from django.core.files import File
from django.core.paginator import Paginator
from django.db.utils import IntegrityError
from django.db import models
from django.db import transaction

from pulpcore.plugin.models import (Artifact, Content, ContentArtifact, RemoteArtifact, Importer,
                                    ProgressBar, Publisher, RepositoryContent, PublishedArtifact,
                                    PublishedMetadata)
from pulpcore.plugin.tasking import Task
from pulpcore.plugin.download.asyncio import Group, GroupDownloader
from pulpcore.plugin.download.file import DownloadedFile
from pulpcore.plugin.download.futures import Batch, DownloadError
from pulpcore.plugin.download.futures.tools import DownloadMonitor

log = getLogger(__name__)

# Changes needed.
Delta = namedtuple('Delta', ('additions', 'removals'))
# Natural key.
Key = namedtuple('Key', ('path', 'digest'))


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
        if not unit_keys:
            return super().get_queryset().none()
        q = models.Q()
        for key in unit_keys:
            unit_key_dict = {}
            for field in ExampleContent.natural_key_fields():
                unit_key_dict[field] = getattr(key, field)
            q |= models.Q(**unit_key_dict)
        if partial:
            return super().get_queryset().filter(q)
        else:
            return super().get_queryset().filter(q).filter(
                contentartifact__artifact__isnull=False)

    @classmethod
    def paginated_qs_results(cls, queryset, page_size=100):
        """
        Paginates the queryset results and emits one ExampleContent object at a time

        This method prevents loading too much data from the database into memory at one time.

        Args:
            queryset (:class:`django.db.models.query.QuerySet`): QuerySet to paginate
            page_size (int): The number of rows to read from the database at one time

        Yields:
            :class:`pulpcore.plugin.models.Content`
        """
        # Need to sort so the paginator returns consistent results
        queryset = queryset.order_by('id')
        paginator = Paginator(queryset, page_size)
        for page_number in paginator.page_range:
            for item in paginator.page(page_number).object_list:
                yield item


class ExampleContent(Content):
    """
    The "example" content type.

    Content of this type represents a 1 files uniquely identified by a path and a SHA256 digest.

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
    def natural_key_fields(cls):
        for unique in cls._meta.unique_together:
            for field in unique:
                yield field


class ExamplePublisher(Publisher):
    """
    A Publisher for ExampleContent.
    """
    TYPE = 'example'

    def publish(self):
        """
        Publish the repository.
        """
        with ProgressBar(message=_("Publishing repository metadata"), total=1) as bar:
            manifest_name = 'PULP_MANIFEST'
            with open(manifest_name, 'w+') as fp:
                for entry in self._publish():
                    fp.write(entry)
                    fp.write('\n')

            metadata = PublishedMetadata(
                relative_path=os.path.basename(manifest_name),
                publication=self.publication,
                file=File(open(manifest_name, 'rb')))
            metadata.save()
            bar.increment()

    def _publish(self):
        """
        Create published artifacts and yield the string representation to be written to the
        PULP_MANIFEST file.

        Yields:
            String: The manifest entry for the published content
        """
        repo_content = ExampleContent.objects.filter(repositories=self.repository)
        with ProgressBar(message=_("Publishing ExampleContent"), total=repo_content.count()) as bar:
            for content in repo_content:
                for content_artifact in content.contentartifact_set.all():
                    artifact = self._find_artifact(content_artifact)
                    published_artifact = PublishedArtifact(
                        relative_path=content_artifact.relative_path,
                        publication=self.publication,
                        content_artifact=content_artifact)
                    published_artifact.save()
                    entry = "{},{},{}".format(content_artifact.relative_path,
                                              artifact.sha256,
                                              artifact.size)
                    yield entry
                bar.increment()

    def _find_artifact(self, content_artifact):
        """
        Find the artifact referenced by a ContentArtifact.

        Args:
            content_artifact (pulpcore.plugin.models.ContentArtifact): A content artifact.

        Returns:
            Artifact: When the artifact exists.
            RemoteArtifact: When the artifact does not exist.
        """
        artifact = content_artifact.artifact
        if not artifact:
            artifact = RemoteArtifact.objects.get(
                content_artifact=content_artifact,
                importer__repository=self.repository)
        return artifact


class ExampleFuturesImporter(Importer):
    """
    Importer for ExampleContent.

    This importer uses the :class:`pulpcore.plugin.download.futures.Batch` to
    download files concurrently. Upon successful download, an Artifact is created. Each Artifact
    is then used to compose ExampleContent and associate it with the repository associated with
    the importer. Any content removed from the remote repository since the previous sync is also
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
        if not self.is_deferred:
            q_set = q_set.filter(contentartifact__artifact__isnull=False)
        q_set = q_set.only(*[field for field in ExampleContent.natural_key_fields()])
        for content in (c.cast() for c in ExampleContent.objects.paginated_qs_results(q_set)):
            key = Key(path=content.path, digest=content.digest)
            inventory.add(key)
        return inventory

    @staticmethod
    def parse(line, line_number):
        """
        Parse the specified line from the manifest into an Entry.

        Args:
            line (str): A line from the manifest
            line_number (int): Line number in the manifest

        Returns:
            Dictionary representing the line from the manifest. The keys are 'path', 'digest',
            and 'size'.

        Raises:
            ValueError: on parsing error.
        """
        part = [s.strip() for s in line.split(',')]
        if len(part) != 3:
            raise ValueError(
                _('Error: manifest line:{n}: '
                  'must be: <path>, <digest>, <size>').format(
                    n=line_number))
        return {'path': part[0], 'digest': part[1], 'size': int(part[2])}

    def read_manifest(self):
        """
        Read the file at `manifest_path` and yield entries.

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
                yield self.parse(line, n)

    def _find_delta(self, mirror=True):
        """
        Using the manifest and set of existing (natural) keys, determine the set of content to be
        added and deleted from the repository.  Expressed in natural key.

        Args:
            mirror (bool): When true, any content removed from remote repository is added to
                the delta.removals. When false, delta.removals is an empty set.

        Returns:
            Delta (namedtuple): The needed changes.
        """
        inventory = self._fetch_inventory()
        parsed_url = urlparse(self.feed_url)

        download = self.get_futures_downloader(self.feed_url, os.path.basename(parsed_url.path))
        download()
        self.manifest_path = download.writer.path
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
        self.monitors = {}
        delta = self._find_delta()

        # Find all content being added that already exists in Pulp and associate with repository.
        fields = {f for f in ExampleContent.natural_key_fields()}
        if not self.is_deferred:
            # Filter out any content that still needs to have artifacts downloaded
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions
                                                                         ).only(*fields)
        else:
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions,
                                                                         partial=True
                                                                         ).only(*fields)
        added = self.associate_existing_content(ready_to_associate)
        remaining_additions = delta.additions - added
        delta = Delta(additions=remaining_additions, removals=delta.removals)

        if self.is_deferred:
            self.deferred_sync(delta)
        else:
            self.full_sync(delta)

        # Remove content if there is any to remove
        if delta.removals:
            # Build a query that uniquely identifies all content that needs to be removed.
            with ProgressBar(message=_("Removing content from repository."), total=len(
                    delta.removals)) as bar:
                q = models.Q()
                for key in delta.removals:
                    q |= models.Q(examplecontent__path=key.path,
                                  examplecontent__digest=key.digest)
                q_set = self.repository.content.filter(q)
                bar.done = RepositoryContent.objects.filter(
                    repository=self.repository).filter(content__in=q_set).delete()[0]

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
        with ProgressBar(message=_("Associating units already in Pulp with the repository"),
                         total=content_q.count()) as bar:
            for content in ExampleContent.objects.paginated_qs_results(content_q):
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
            delta (namedtuple): Set of unit keys for units to be added to the repository. Set
                of unit keys for units that should be removed from the repository. Only the
                additions are used in this method.
        """
        description = _("Adding file content to the repository without downloading artifacts.")
        progress_bar = ProgressBar(message=description, total=len(delta.additions))

        with progress_bar:
            for remote_artifact in self.next_remote_artifact(delta.additions):
                content = self.content_dict.pop(remote_artifact.url)
                self._create_and_associate_content(content, {remote_artifact: None})
                progress_bar.increment()

    def full_sync(self, delta):
        """
        Synchronize the repository with the remote repository and download artifacts.

        Args:
            delta (namedtuple): Set of unit keys for units to be added to the repository. Set
                of unit keys for units that should be removed from the repository. Only the
                additions are used in this method.
        """
        description = _("Dowloading artifacts and adding content to the repository.")
        current_task = Task()
        with ProgressBar(message=description, total=len(delta.additions)) as bar:
            with Batch(self.next_download(delta.additions)) as batch:
                for plan in batch():
                    try:
                        plan.result()
                    except DownloadError as e:
                        current_task.append_non_fatal_error(e)
                    else:
                        content = self.content_dict.pop(plan.download.url)
                        monitor_dict = self.monitors.pop(plan.download.url).facts()
                        monitor_dict.update({'path': plan.download.writer.path})
                        self._create_and_associate_content(content,
                                                           {plan.download.attachment: monitor_dict})
                        bar.increment()

    def next_download(self, additions):
        """
        Generator of ExampleContent, ContentArtifacts, and RemoteArtifacts.

        This generator is responsible for creating all the models needed to create ExampleContent in
        Pulp. It stores the ExampleContent in a dictionary to be used after all the related
        Artifacts have been downloaded. This generator emits a HttpDownload object.

        Args:
            additions (set of namedtuple Key): Set of Keys corresponding to ExampleContent that
                should be downloaded.

        Yields:
            HttpDownload used for downloading an Artifact for ExampleContent.
        """
        for remote_artifact in self.next_remote_artifact(additions):
            url = remote_artifact.url
            download = self.get_futures_downloader(remote_artifact.url, self.content_dict[url].path,
                                                   remote_artifact)
            download.attachment = remote_artifact
            self.monitors[url] = DownloadMonitor(download)
            yield download

    def next_remote_artifact(self, additions):
        """
        Generator of ExampleContent, ContentArtifacts, and RemoteArtifacts.

        This generator is responsible for creating all the models needed to create ExampleContent in
        Pulp. It stores the ExampleContent in a dictionary to be used in the deferred_sync
        method. This generator emits a RemoteArtifact object.

        Args:
            additions (set of namedtuple Key): Set of Keys corresponding to ExampleContent that
                should be created.

        Yields:
            RemoteArtifact that is needed for the ExampleContent.
        """
        parsed_url = urlparse(self.feed_url)
        root_dir = os.path.dirname(parsed_url.path)
        for entry in self.read_manifest():
            key = Key(path=entry['path'], digest=entry['digest'])
            if key in additions:
                path = os.path.join(root_dir, entry['path'])
                url = urlunparse(parsed_url._replace(path=path))
                example_content = ExampleContent(path=entry['path'], digest=entry['digest'])
                self.content_dict[url] = example_content
                # The content is set on the content_artifact right before writing to the
                # database. This helps deal with race conditions when saving Content.
                content_artifact = ContentArtifact(relative_path=entry['path'])
                remote_artifact = RemoteArtifact(url=url,
                                                 importer=self,
                                                 sha256=entry['digest'],
                                                 size=entry['size'],
                                                 content_artifact=content_artifact)
                yield remote_artifact

    def _create_and_associate_content(self, content, group_result):
        """
        Saves ExampleContent and all related models to the database

        This method saves ExampleContent, ContentArtifacts, RemoteArtifacts and Artifacts to
        the database inside a single transaction.

        Args:
            content (:class:`pulp_example.app.models.ExampleContent`): An instance of
                ExampleContent to be saved to the database.
            group_result (dict): A dictionary where keys are instances of
                :class:`pulpcore.plugin.models.RemoteArtifact` and values are dictionaries that
                contain information about files downloaded using the RemoteArtifacts.
        """

        # Save Artifacts, ContentArtifacts, RemoteArtifacts, and Content in a transaction
        with transaction.atomic():
            # Save content
            try:
                with transaction.atomic():
                    content.save()
                    log.warning(_("Created content"))
            except IntegrityError:
                key = {f: getattr(content, f) for f in
                       content.natural_key_fields()}
                content = type(content).objects.get(**key)
            try:
                with transaction.atomic():
                    # Add content to the repository
                    association = RepositoryContent(
                        repository=self.repository,
                        content=content)
                    association.save()
                    log.warning(_("Created association with repository"))
            except IntegrityError:
                # Content unit is already associated with the repository
                pass

            for remote_artifact, download_result in group_result.items():
                if download_result:
                    # Create artifact that was downloaded and deal with race condition
                    path = download_result.pop('path')
                    with DownloadedFile(open(path, mode='rb')) as file:
                        try:
                            with transaction.atomic():
                                artifact = Artifact(file=file,
                                                    **download_result)
                                artifact.save()
                        except IntegrityError:
                            artifact = Artifact.objects.get(sha256=download_result['sha256'])
                else:
                    # Try to find an artifact if one already exists
                    try:
                        with transaction.atomic():
                            # try to find an artifact from information in remote artifact
                            artifact = Artifact.objects.get(sha256=remote_artifact.sha256)
                    except Artifact.DoesNotExist:
                        artifact = None
                content_artifact = remote_artifact.content_artifact
                content_artifact.artifact = artifact
                content_artifact.content = content
                try:
                    with transaction.atomic():
                        content_artifact.save()
                except IntegrityError:
                    content_artifact = ContentArtifact.objects.get(
                        content=content_artifact.content,
                        relative_path=content_artifact.relative_path)
                    remote_artifact.content_artifact = content_artifact
                    content_artifact.artifact = artifact
                    content_artifact.save()
                try:
                    with transaction.atomic():
                        remote_artifact.save()
                except IntegrityError:
                    pass


class ExampleAsyncIOImporter(Importer):
    """
    Importer for ExampleContent.

    This importer uses the :class:`pulpcore.plugin.download.asyncio.GroupDownloader` to
    download files concurrently. Upon successful download, an Artifact is created. Each Artifact
    is then used to compose ExampleContent and associate it with the repository associated with
    the importer. Any content removed from the remote repository since the previous sync is also
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
        if not self.is_deferred:
            q_set = q_set.filter(contentartifact__artifact__isnull=False)
        q_set = q_set.only(*[field for field in ExampleContent.natural_key_fields()])
        for content in (c.cast() for c in ExampleContent.objects.paginated_qs_results(q_set)):
            key = Key(path=content.path, digest=content.digest)
            inventory.add(key)
        return inventory

    @staticmethod
    def parse(line, line_number):
        """
        Parse the specified line from the manifest into an Entry.

        Args:
            line (str): A line from the manifest
            line_number (int): Line number in the manifest

        Returns:
            Dictionary representing the line from the manifest. The keys are 'path', 'digest',
            and 'size'.

        Raises:
            ValueError: on parsing error.
        """
        part = [s.strip() for s in line.split(',')]
        if len(part) != 3:
            raise ValueError(
                _('Error: manifest line:{n}: '
                  'must be: <path>, <digest>, <size>').format(
                    n=line_number))
        return {'path': part[0], 'digest': part[1], 'size': int(part[2])}

    def read_manifest(self):
        """
        Read the file at `manifest_path` and yield entries.

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
                yield self.parse(line, n)

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
        download = self.get_asyncio_downloader(self.feed_url)
        download_result = download.fetch()
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
        fields = {f for f in ExampleContent.natural_key_fields()}
        if not self.is_deferred:
            # Filter out any content that still needs to have artifacts downloaded
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions).only(
                *fields)
        else:
            ready_to_associate = ExampleContent.objects.find_by_unit_key(delta.additions,
                                                                         partial=True
                                                                         ).only(*fields)
        added = self.associate_existing_content(ready_to_associate)
        remaining_additions = delta.additions - added
        delta = Delta(additions=remaining_additions, removals=delta.removals)

        if self.is_deferred:
            self.deferred_sync(delta)
        else:
            self.full_sync(delta)

        # Remove content if there is any to remove
        if delta.removals:
            # Build a query that uniquely identifies all content that needs to be removed.
            q = models.Q()
            for key in delta.removals:
                q |= models.Q(examplecontent__path=key.path,
                              examplecontent__digest=key.digest)
            q_set = self.repository.content.filter(q)
            RepositoryContent.objects.filter(
                repository=self.repository).filter(content__in=q_set).delete()

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
        with ProgressBar(message=_("Associating units already in Pulp with the repository"),
                         total=content_q.count()) as bar:
            for content in ExampleContent.objects.paginated_qs_results(content_q):
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

        with ProgressBar(message=description, total=len(delta.additions)) as bar:
            for group in self.next_group(delta.additions):
                self._create_and_associate_content(group)
                bar.increment()

    def full_sync(self, delta):
        """
        Synchronize the repository with the remote repository without downloading artifacts.
        """
        description = _("Dowloading artifacts and adding content to the repository.")
        downloader = GroupDownloader(self)
        downloader.schedule_from_iterator(self.next_group(delta.additions))

        with ProgressBar(message=description, total=len(delta.additions)) as bar:
            for group in downloader:
                download_error = False
                for url, result in group.downloaded_files.items():
                    if result.exception:
                        download_error = True
                if not download_error:
                    self._create_and_associate_content(group)
                    bar.increment()
                    log.warning('content_unit = {0}'.format(group.id))

    def next_group(self, additions):
        """
        Generator of ExampleContent, ContentArtifacts, and RemoteArtifacts.

        This generator is responsible for creating all the models needed to create ExampleContent in
        Pulp. The ExampleContent object is stored in a dictionary so it can be referenced after
        downloads complete. This generator emits a
        :class:`pulpcore.plugin.download.asyncio.group.Group`.
        """
        parsed_url = urlparse(self.feed_url)
        root_dir = os.path.dirname(parsed_url.path)
        for entry in self.read_manifest():
            key = Key(path=entry['path'], digest=entry['digest'])
            if key in additions:
                path = os.path.join(root_dir, entry['path'])
                url = urlunparse(parsed_url._replace(path=path))
                example_content = ExampleContent(path=entry['path'], digest=entry['digest'])
                content_id = tuple(getattr(example_content, f) for f in
                                   example_content.natural_key_fields())
                self.content_dict[content_id] = example_content
                # The content is set on the content_artifact right before writing to the
                # database. This helps deal with race conditions when saving Content.
                content_artifact = ContentArtifact(relative_path=entry['path'])
                remote_artifacts = [RemoteArtifact(url=url,
                                                   importer=self,
                                                   sha256=entry['digest'],
                                                   size=entry['size'],
                                                   content_artifact=content_artifact)]
                yield Group(content_id, remote_artifacts)

    def _create_and_associate_content(self, group):
        """
        Saves ExampleContent and all related models to the database.

        This method saves ExampleContent, ContentArtifacts, RemoteArtifacts and Artifacts to
        the database inside a single transaction.

        Args:
            content (:class:`pulp_example.app.models.ExampleContent`): An instance of
                ExampleContent to be saved to the database.
            deferred_artifacts (dict): A dictionary where keys are instances of
                :class:`pulpcore.plugin.models.RemoteArtifact` and values are dictionaries that
                contain information about files downloaded using the RemoteArtifacts.
        """

        # Save Artifacts, ContentArtifacts, RemoteArtifacts, and Content in a transaction
        content = self.content_dict.pop(group.id)
        with transaction.atomic():
            # Save content
            try:
                with transaction.atomic():
                    content.save()
                    log.debug(_("Created content"))
            except IntegrityError:
                key = {f: getattr(content, f) for f in
                       content.natural_key_fields()}
                content = type(content).objects.get(**key)
            try:
                with transaction.atomic():
                    # Add content to the repository
                    association = RepositoryContent(
                        repository=self.repository,
                        content=content)
                    association.save()
                    log.debug(_("Created association with repository"))
            except IntegrityError:
                # Content is already associated with the repository
                pass
            for url in group.urls:
                if group.downloaded_files:
                    # Create artifact that was downloaded and deal with race condition
                    with DownloadedFile(open(group.downloaded_files[url].path, mode='rb')) as file:
                        try:
                            with transaction.atomic():
                                artifact = Artifact(file=file,
                                                    **group.downloaded_files[
                                                        url].artifact_attributes)
                                artifact.save()
                        except IntegrityError:
                            artifact = Artifact.objects.get(sha256=group.downloaded_files[
                                url].artifact_attributes['sha256'])
                else:
                    # Try to find an artifact if one already exists
                    try:
                        with transaction.atomic():
                            # try to find an artifact from information in deferred artifact
                            artifact = Artifact.objects.get(sha256=group.remote_artifacts[
                                url].sha256)
                    except Artifact.DoesNotExist:
                        artifact = None
                content_artifact = group.remote_artifacts[url].content_artifact
                content_artifact.artifact = artifact
                content_artifact.content = content
                try:
                    with transaction.atomic():
                        content_artifact.save()
                except IntegrityError:
                    content_artifact = ContentArtifact.objects.get(
                        content=content_artifact.content,
                        relative_path=content_artifact.relative_path)
                    group.remote_artifacts[url].content_artifact = content_artifact
                    content_artifact.artifact = artifact
                    content_artifact.save()
                try:
                    with transaction.atomic():
                        group.remote_artifacts[url].save()
                except IntegrityError:
                    pass
