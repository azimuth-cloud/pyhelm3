import datetime
import enum
import typing as t

import yaml

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    DirectoryPath,
    FilePath,
    AnyUrl,
    HttpUrl,
    constr
)

from .command import Command, SafeLoader


class ModelWithCommand(BaseModel):
    """
    Base class for a model that has a Helm command object.
    """
    # The command object that is used to invoke Helm
    _command: Command = PrivateAttr()

    def __init__(self, _command: Command, **kwargs):
        super().__init__(**kwargs)
        self._command = _command


#: Type for a non-empty string
NonEmptyString = constr(min_length = 1)


#: Type for a name (chart or release)
Name = constr(regex = r"^[a-z0-9-]+$")


#: Type for a SemVer version
SemVerVersion = constr(regex = r"^v?\d+\.\d+\.\d+(-[a-zA-Z0-9\.\-]+)?(\+[a-zA-Z0-9\.\-]+)?$")


#: Type variables for forward references to the chart and release types
ChartType = t.TypeVar("ChartType", bound = "Chart")
ReleaseType = t.TypeVar("ReleaseType", bound = "Release")
ReleaseRevisionType = t.TypeVar("ReleaseRevisionType", bound = "ReleaseRevision")


class ChartDependency(BaseModel):
    """
    Model for a chart dependency.
    """
    name: Name = Field(
        ...,
        description = "The name of the chart."
    )
    version: NonEmptyString = Field(
        ...,
        description = "The version of the chart. Can be a SemVer range."
    )
    repository: t.Optional[NonEmptyString] = Field(
        None,
        description = "The repository URL or alias."
    )
    condition: t.Optional[NonEmptyString] = Field(
        None,
        description = "A yaml path that resolves to a boolean, used for enabling/disabling the chart."
    )
    tags: t.List[NonEmptyString] = Field(
        default_factory = list,
        description = "Tags can be used to group charts for enabling/disabling together."
    )
    import_values: t.List[t.Union[t.Dict[str, str], str]] = Field(
        default_factory = list,
        alias = "import-values",
        description = (
            "Mapping of source values to parent key to be imported. "
            "Each item can be a string or pair of child/parent sublist items."
        )
    )
    alias: t.Optional[NonEmptyString] = Field(
        None,
        description = "Alias to be used for the chart."
    )


class ChartMaintainer(BaseModel):
    """
    Model for the maintainer of a chart.
    """
    name: NonEmptyString = Field(
        ...,
        description = "The maintainer's name."
    )
    email: t.Optional[NonEmptyString] = Field(
        None,
        description = "The maintainer's email."
    )
    url: t.Optional[AnyUrl] = Field(
        None,
        description = "A URL for the maintainer."
    )


class ChartMetadata(BaseModel):
    """
    Model for chart metadata, from Chart.yaml.
    """
    api_version: t.Literal["v1", "v2"] = Field(
        ...,
        alias = "apiVersion",
        description = "The chart API version."
    )
    name: Name = Field(
        ...,
        description = "The name of the chart."
    )
    version: SemVerVersion = Field(
        ...,
        description = "The version of the chart."
    )
    kube_version: t.Optional[NonEmptyString] = Field(
        None,
        alias = "kubeVersion",
        description = "A SemVer range of compatible Kubernetes versions for the chart."
    )
    description: t.Optional[NonEmptyString] = Field(
        None,
        description = "A single-sentence description of the chart."
    )
    type: t.Literal["application", "library"] = Field(
        "application",
        description = "The type of the chart."
    )
    keywords: t.List[NonEmptyString] = Field(
        default_factory = list,
        description = "List of keywords for the chart."
    )
    home: t.Optional[HttpUrl] = Field(
        None,
        description = "The URL of th home page for the chart."
    )
    sources: t.List[AnyUrl] = Field(
        default_factory = list,
        description = "List of URLs to source code for this chart."
    )
    dependencies: t.List[ChartDependency] = Field(
        default_factory = list,
        description = "List of the chart dependencies."
    )
    maintainers: t.List[ChartMaintainer] = Field(
        default_factory = list,
        description = "List of maintainers for the chart."
    )
    icon: t.Optional[HttpUrl] = Field(
        None,
        description = "URL to an SVG or PNG image to be used as an icon."
    )
    app_version: t.Optional[NonEmptyString] = Field(
        None,
        alias = "appVersion",
        description = (
            "The version of the app that this chart deploys. "
            "SemVer is not required."
        )
    )
    deprecated: bool = Field(
        False,
        description = "Whether this chart is deprecated."
    )
    annotations: t.Dict[str, str] = Field(
        default_factory = dict,
        description = "Annotations for the chart."
    )


class Chart(ModelWithCommand):
    """
    Model for a reference to a chart.
    """
    ref: t.Union[DirectoryPath, FilePath, HttpUrl, Name] = Field(
        ...,
        description = (
            "The chart reference. "
            "Can be a chart directory or a packaged chart archive on the local "
            "filesystem, the URL of a packaged chart or the name of a chart. "
            "When a name is given, repo must also be given and version may optionally "
            "be given."
        )
    )
    repo: t.Optional[HttpUrl] = Field(None, description = "The repository URL.")
    metadata: ChartMetadata = Field(..., description = "The metadata for the chart.")

    # Private attributes used to cache attributes
    _readme: str = PrivateAttr(None)
    _crds: t.List[t.Dict[str, t.Any]] = PrivateAttr(None)
    _values: t.Dict[str, t.Any] = PrivateAttr(None)

    async def readme(self) -> str:
        """
        Returns the README for the chart.
        """
        if self._readme is None:
            self._readme = await self._command.show_readme(
                self.ref,
                repo = self.repo,
                version = self.metadata.version
            )
        return self._readme

    async def crds(self) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the CRDs for the chart.
        """
        if self._crds is None:
            self._crds = list(
                await self._command.show_crds(
                    self.ref,
                    repo = self.repo,
                    version = self.metadata.version
                )
            )
        return self._crds

    async def values(self) -> t.Dict[str, t.Any]:
        """
        Returns the values for the chart.
        """
        if self._values is None:
            self._values = await self._command.show_values(
                self.ref,
                repo = self.repo,
                version = self.metadata.version
            )
        return self._values


class Release(ModelWithCommand):
    """
    Model for a Helm release.
    """
    name: Name = Field(
        ...,
        description = "The name of the release."
    )
    namespace: Name = Field(
        ...,
        description = "The namespace of the release." 
    )

    async def current_revision(self) -> ReleaseRevisionType:
        """
        Returns the current revision for the release.
        """
        return ReleaseRevision._from_status(
            await self._command.status(
                self.name,
                namespace = self.namespace
            ),
            self._command
        )

    async def revision(self, revision: int) -> ReleaseRevisionType:
        """
        Returns the specified revision for the release.
        """
        return ReleaseRevision._from_status(
            await self._command.status(
                self.name,
                namespace = self.namespace,
                revision = revision
            ),
            self._command
        )

    async def history(self, max_revisions: int = 256) -> t.Iterable[ReleaseRevisionType]:
        """
        Returns all the revisions for the release.
        """
        history = await self._command.history(
            self.name,
            max_revisions = max_revisions,
            namespace = self.namespace
        )
        return (
            ReleaseRevision(
                self._command,
                release = self,
                revision = revision["revision"],
                status = revision["status"],
                updated = revision["updated"],
                description = revision.get("description")
            )
            for revision in history
        )

    async def rollback(
        self,
        revision: int,
        *,
        cleanup_on_fail: bool = False,
        dry_run: bool = False,
        force: bool = False,
        no_hooks: bool = False,
        recreate_pods: bool = False,
        timeout: t.Union[int, str] = "5m",
        wait: bool = False
    ) -> ReleaseRevisionType:
        """
        Rollback this release to the specified version and return the revision that was created.
        """
        await self._command.rollback(
            self.name,
            revision,
            cleanup_on_fail = cleanup_on_fail,
            dry_run = dry_run,
            force = force,
            namespace = self.namespace,
            no_hooks = no_hooks,
            recreate_pods = recreate_pods,
            timeout = timeout,
            wait = wait
        )
        return await self.current_revision()

    async def uninstall(
        self,
        *,
        dry_run: bool = False,
        keep_history: bool = False,
        no_hooks: bool = False,
        timeout: t.Union[int, str] = "5m",
        wait: bool = False
    ):
        """
        Uninstalls this release.
        """
        await self._command.uninstall(
            self.name,
            dry_run = dry_run,
            keep_history = keep_history,
            namespace = self.namespace,
            no_hooks = no_hooks,
            timeout = timeout,
            wait = wait
        )


class ReleaseRevisionStatus(str, enum.Enum):
    """
    Enumeration of possible release statuses.
    """
    #: Indicates that the revision is in an uncertain state
    UNKNOWN = "unknown"
    #: Indicates that the revision has been pushed to Kubernetes
    DEPLOYED = "deployed"
    #: Indicates that the revision has been uninstalled from Kubernetes
    UNINSTALLED = "uninstalled"
    #: Indicates that the revision is outdated and a newer one exists
    SUPERSEDED = "superseded"
    #: Indicates that the revision was not successfully deployed
    FAILED = "failed"
    #: Indicates that an uninstall operation is underway for this revision
    UNINSTALLING = "uninstalling"
    #: Indicates that an install operation is underway for this revision
    PENDING_INSTALL = "pending-install"
    #: Indicates that an upgrade operation is underway for this revision
    PENDING_UPGRADE = "pending-upgrade"
    #: Indicates that a rollback operation is underway for this revision
    PENDING_ROLLBACK = "pending-rollback"


class HookEvent(str, enum.Enum):
    """
    Enumeration of possible hook events.
    """
    PRE_INSTALL = "pre-install"
    POST_INSTALL = "post-install"
    PRE_DELETE = "pre-delete"
    POST_DELETE = "post-delete"
    PRE_UPGRADE = "pre-upgrade"
    POST_UPGRADE = "post-upgrade"
    PRE_ROLLBACK = "pre-rollback"
    POST_ROLLBACK = "post-rollback"
    TEST = "test"


class HookDeletePolicy(str, enum.Enum):
    """
    Enumeration of possible delete policies for a hook.
    """
    HOOK_SUCCEEDED = "hook-succeeded"
    HOOK_FAILED = "hook-failed"
    HOOK_BEFORE_HOOK_CREATION = "before-hook-creation"


class HookPhase(str, enum.Enum):
    """
    Enumeration of possible phases for a hook.
    """
    #: Indicates that a hook is in an unknown state
    UNKNOWN = "Unknown"
    #: Indicates that a hook is currently executing
    RUNNING = "Running"
    #: Indicates that hook execution succeeded
    SUCCEEDED = "Succeeded"
    #: Indicates that hook execution failed
    FAILED = "Failed"


class Hook(BaseModel):
    """
    Model for a hook.
    """
    name: NonEmptyString = Field(
        ...,
        description = "The name of the hook."
    )
    phase: HookPhase = Field(
        HookPhase.UNKNOWN,
        description = "The phase of the hook."
    )
    kind: NonEmptyString = Field(
        ...,
        description = "The kind of the hook."
    )
    path: NonEmptyString = Field(
        ...,
        description = "The chart-relative path to the template that produced the hook."
    )
    resource: t.Dict[str, t.Any] = Field(
        ...,
        description = "The resource for the hook."
    )
    events: t.List[HookEvent] = Field(
        default_factory = list,
        description = "The events that the hook fires on."
    )
    delete_policies: t.List[HookDeletePolicy] = Field(
        default_factory = list,
        description = "The delete policies for the hook."
    )


class ReleaseRevision(ModelWithCommand):
    """
    Model for a revision of a release.
    """
    release: ReleaseType = Field(
        ...,
        description = "The parent release of this revision."
    )
    revision: int = Field(
        ...,
        description = "The revision number of this revision."
    )
    status: ReleaseRevisionStatus = Field(
        ...,
        description = "The status of the revision."
    )
    updated: datetime.datetime = Field(
        ...,
        description = "The time at which this revision was updated."
    )
    description: t.Optional[NonEmptyString] = Field(
        None,
        description = "'Log entry' for this revision."
    )
    notes: t.Optional[NonEmptyString] = Field(
        None,
        description = "The rendered notes for this revision, if available."
    )

    # Optional fields if they are known at creation time
    chart_metadata_: t.Optional[ChartMetadata] = Field(None, alias = "chart_metadata")
    hooks_: t.Optional[t.List[t.Dict[str, t.Any]]] = Field(None, alias = "hooks")
    resources_: t.Optional[t.List[t.Dict[str, t.Any]]] = Field(None, alias = "resources")
    values_: t.Optional[t.Dict[str, t.Any]] = Field(None, alias = "values")

    def _set_from_status(self, status):
        # Statuses from install/upgrade have chart metadata embedded
        if "chart" in status:
            self.chart_metadata_ = ChartMetadata(**status["chart"]["metadata"])
        self.hooks_ = [
            Hook(
                name = hook["name"],
                phase = hook["last_run"].get("phase") or "Unknown",
                kind = hook["kind"],
                path = hook["path"],
                resource = yaml.load(hook["manifest"], Loader = SafeLoader),
                events = hook["events"],
                delete_policies = hook.get("delete_policies", [])
            )
            for hook in status["hooks"]
        ]
        self.resources_ = list(yaml.load_all(status["manifest"], Loader = SafeLoader))

    async def _init_from_status(self):
        self._set_from_status(
            await self._command.status(
                self.release.name,
                namespace = self.release.namespace,
                revision = self.revision
            )
        )

    async def chart_metadata(self) -> ChartMetadata:
        """
        Returns the metadata for the chart that was used for this revision.
        """
        if self.chart_metadata_ is None:
            metadata = await self._command.get_chart_metadata(
                self.release.name,
                namespace = self.release.namespace,
                revision = self.revision
            )
            self.chart_metadata_ = ChartMetadata(**metadata)
        return self.chart_metadata_
    
    async def hooks(self) -> t.Iterable[Hook]:
        """
        Returns the hooks that were executed as part of this revision.
        """
        if self.hooks_ is None:
            await self._init_from_status()
        return self.hooks_

    async def resources(self) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the resources that were created as part of this revision.
        """
        if self.resources_ is None:
            await self._init_from_status()
        return self.resources_

    async def values(self, computed: bool = False) -> t.Dict[str, t.Any]:
        """
        Returns the values that were used for this revision.
        """
        return await self._command.get_values(
            self.release.name,
            computed = computed,
            namespace = self.release.namespace,
            revision = self.revision
        )

    async def refresh(self) -> ReleaseRevisionType:
        """
        Returns a new revision representing the most recent state of this revision.
        """
        return self.__class__._from_status(
            await self._command.status(
                self.release.name,
                namespace = self.release.namespace,
                revision = self.revision
            ),
            self._command
        )

    @classmethod
    def _from_status(cls, status: t.Dict[str, t.Any], command: Command):
        """
        Internal constructor to create a release revision from a status result.
        """
        revision = ReleaseRevision(
            command,
            release = Release(
                command,
                name = status["name"],
                namespace = status["namespace"]
            ),
            revision = status["version"],
            status = status["info"]["status"],
            updated = status["info"]["last_deployed"],
            description = status["info"].get("description"),
            notes = status["info"].get("notes")
        )
        revision._set_from_status(status)
        return revision
