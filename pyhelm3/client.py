import contextlib
import pathlib
import shutil
import typing as t

import yaml

from .command import Command, SafeLoader
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
        /,
        default_timeout: t.Union[int, str] = "5m",
        executable: str = "helm",
        history_max_revisions: int = 10,
        insecure_skip_tls_verify: bool = False,
        kubeconfig: t.Optional[pathlib.Path] = None,
        unpack_directory: t.Optional[str] = None
    ):
        self._command = command or Command(
            default_timeout = default_timeout,
            executable = executable,
            history_max_revisions = history_max_revisions,
            insecure_skip_tls_verify = insecure_skip_tls_verify,
            kubeconfig = kubeconfig,
            unpack_directory = unpack_directory
        )

    async def get_chart(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        /,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> Chart:
        """
        Returns the resolved chart for the given ref, repo and version.
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

    @contextlib.asynccontextmanager
    async def pull_chart(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        /,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> contextlib.AbstractAsyncContextManager[pathlib.Path]:
        """
        Context manager that pulls the specified chart and yields a chart object
        whose ref is the unpacked chart directory.

        Ensures that the directory is cleaned up when the context manager exits.
        """
        path = await self._command.pull(
            chart_ref,
            devel = devel,
            repo = repo,
            version = version
        )
        try:
            # The path from pull is the managed directory containing the archive and unpacked chart
            # We want the actual chart directory
            chart_yaml = next(path.glob("**/Chart.yaml"))
            chart_directory = chart_yaml.parent
            # To save the overhead of another Helm command invocation, just read the Chart.yaml
            with chart_yaml.open() as fh:
                metadata = yaml.load(fh, Loader = SafeLoader)
            # Yield the chart object
            yield Chart(self._command, ref = chart_directory, metadata = metadata)
        finally:
            if path.is_dir():
                shutil.rmtree(path)

    async def template_resources(
        self,
        chart: Chart,
        release_name: str,
        values: t.Optional[t.Dict[str, t.Any]] = None,
        /,
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
        /,
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
        /,
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
        /,
        atomic: bool = False,
        cleanup_on_fail: bool = False,
        create_namespace: bool = True,
        description: t.Optional[str] = None,
        dry_run: bool = False,
        force: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        reset_values: bool = False,
        reuse_values: bool = False,
        skip_crds: bool = False,
        timeout: t.Union[int, str, None] = None,
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
                create_namespace = create_namespace,
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

    async def uninstall_release(
        self,
        release_name: str,
        /,
        dry_run: bool = False,
        keep_history: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        timeout: t.Union[int, str, None] = None,
        wait: bool = False
    ):
        """
        Uninstall the named release.
        """
        await self._command.uninstall(
            release_name,
            dry_run = dry_run,
            keep_history = keep_history,
            namespace = namespace,
            no_hooks = no_hooks,
            timeout = timeout,
            wait = wait
        )
