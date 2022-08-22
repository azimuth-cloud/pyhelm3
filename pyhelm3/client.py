import pathlib
import typing as t

from .command import Command
from .models import Chart, Release, ReleaseRevision


#: Bound type var for forward references
ClientType = t.TypeVar("ClientType", bound = "Client")


class Client:
    """
    Entrypoint for interactions with Helm.
    """
    def __init__(
        self,
        command: t.Optional[Command] = None,
        *,
        executable: str = "helm",
        history_max_revisions: int = 10,
        insecure_skip_tls_verify: bool = False,
        kubeconfig: t.Optional[pathlib.Path] = None,
        unpack_directory: t.Optional[str] = None
    ):
        self._command = command or Command(
            executable = executable,
            history_max_revisions = history_max_revisions,
            insecure_skip_tls_verify = insecure_skip_tls_verify,
            kubeconfig = kubeconfig,
            unpack_directory = unpack_directory
        )

    def reconfigure(
        self,
        *,
        executable: t.Optional[str] = None,
        history_max_revisions: t.Optional[int] = None,
        insecure_skip_tls_verify: t.Optional[bool] = None,
        kubeconfig: t.Optional[pathlib.Path] = None,
        unpack_directory: t.Optional[str] = None
    ) -> ClientType:
        """
        Returns a new client based on this one but with the specified reconfiguration.

        In particular, the new client shares a lock with this one.
        """
        command = self._command.reconfigure(
            executable = executable,
            history_max_revisions = history_max_revisions,
            insecure_skip_tls_verify = insecure_skip_tls_verify,
            kubeconfig = kubeconfig,
            unpack_directory = unpack_directory
        )
        return self.__class__(command)

    async def get_chart(
        self,
        chart_ref: str,
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> Chart:
        """
        Returns the resolved chart for the given ref, chart and version.
        """
        return Chart(
            self._command,
            ref = chart_ref,
            repo = repo,
            # Load the metadata for the specified args
            metadata = await self._command.show_chart(
                chart_ref,
                devel = devel,
                repo = repo,
                version = version
            )
        )

    async def template_resources(
        self,
        chart: Chart,
        release_name: str,
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        include_crds: bool = False,
        is_upgrade: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Renders the templates from the given chart with the given values and returns
        the resources that would be produced.
        """
        return await self._command.template(
            release_name,
            chart.ref,
            values,
            include_crds = include_crds,
            is_upgrade = is_upgrade,
            namespace = namespace,
            no_hooks = no_hooks,
            repo = chart.repo,
            version = chart.metadata.version
        )

    async def list_releases(
        self,
        *,
        all: bool = False,
        all_namespaces: bool = False,
        include_deployed: bool = True,
        include_failed: bool = False,
        include_pending: bool = False,
        include_superseded: bool = False,
        include_uninstalled: bool = False,
        include_uninstalling: bool = False,
        max_releases: int = 256,
        namespace: t.Optional[str] = None,
        sort_by_date: bool = False,
        sort_reversed: bool = False
    ) -> t.Iterable[Release]:
        """
        Returns an iterable of the deployed releases.
        """
        return (
            Release(
                self._command,
                name = release["name"],
                namespace = release["namespace"],
            )
            for release in await self._command.list(
                all = all,
                all_namespaces = all_namespaces,
                include_deployed = include_deployed,
                include_failed = include_failed,
                include_pending = include_pending,
                include_superseded = include_superseded,
                include_uninstalled = include_uninstalled,
                include_uninstalling = include_uninstalling,
                max_releases = max_releases,
                namespace = namespace,
                sort_by_date = sort_by_date,
                sort_reversed = sort_reversed
            )
        )

    async def get_current_revision(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None
    ) -> ReleaseRevision:
        """
        Returns the current revision of the named release.
        """
        return ReleaseRevision._from_status(
            await self._command.status(
                release_name,
                namespace = namespace
            ),
            self._command
        )

    async def install_or_upgrade_release(
        self,
        release_name: str,
        chart: Chart,
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        atomic: bool = False,
        cleanup_on_fail: bool = False,
        description: t.Optional[str] = None,
        dry_run: bool = False,
        force: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        reset_values: bool = False,
        reuse_values: bool = False,
        skip_crds: bool = False,
        timeout: t.Union[int, str] = "5m",
        wait: bool = False
    ) -> ReleaseRevision:
        """
        Install or upgrade the named release using the given chart and values and return
        the new revision.
        """
        return ReleaseRevision._from_status(
            await self._command.install_or_upgrade(
                release_name,
                chart.ref,
                values,
                atomic = atomic,
                cleanup_on_fail = cleanup_on_fail,
                description = description,
                dry_run = dry_run,
                force = force,
                namespace = namespace,
                no_hooks = no_hooks,
                repo = chart.repo,
                reset_values = reset_values,
                reuse_values = reuse_values,
                skip_crds = skip_crds,
                timeout = timeout,
                version = chart.metadata.version,
                wait = wait
            ),
            self._command
        )
