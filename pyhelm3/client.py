import contextlib
import functools
import pathlib
import shutil
import typing as t

import yaml

from .command import Command, SafeLoader
from .errors import ReleaseNotFoundError
from .models import Chart, Release, ReleaseRevision, ReleaseRevisionStatus, ChartVersion


def mergeconcat(
    defaults: t.Dict[t.Any, t.Any],
    *overrides: t.Dict[t.Any, t.Any]
) -> t.Dict[t.Any, t.Any]:
    """
    Deep-merge two or more dictionaries together. Lists are concatenated.
    """
    def mergeconcat2(defaults, overrides):
        if isinstance(defaults, dict) and isinstance(overrides, dict):
            merged = dict(defaults)
            for key, value in overrides.items():
                if key in defaults:
                    merged[key] = mergeconcat2(defaults[key], value)
                else:
                    merged[key] = value
            return merged
        elif isinstance(defaults, (list, tuple)) and isinstance(overrides, (list, tuple)):
            merged = list(defaults)
            merged.extend(overrides)
            return merged
        else:
            return overrides if overrides is not None else defaults
    return functools.reduce(mergeconcat2, overrides, defaults)


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
        default_timeout: t.Union[int, str] = "5m",
        executable: str = "helm",
        history_max_revisions: int = 10,
        insecure_skip_tls_verify: bool = False,
        kubeconfig: t.Optional[pathlib.Path] = None,
        kubecontext: t.Optional[str] = None,
        unpack_directory: t.Optional[str] = None
    ):
        self._command = command or Command(
            default_timeout = default_timeout,
            executable = executable,
            history_max_revisions = history_max_revisions,
            insecure_skip_tls_verify = insecure_skip_tls_verify,
            kubeconfig = kubeconfig,
            kubecontext = kubecontext,
            unpack_directory = unpack_directory
        )

    async def get_chart(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
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
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> t.AsyncIterator[pathlib.Path]:
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
        *values: t.Dict[str, t.Any],
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
            mergeconcat(*values) if values else None,
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

    async def search_chart(
        self,
        search_keyword: str = None,
        all_versions: bool = False,
        devel: bool = False,        
    ) -> t.Iterable[ChartVersion]:
        """
        Returns an iterable of the available versions.
        """
        return (
            ChartVersion(
                self._command,
                name = release["name"],
                version = release["version"],
                description = release["description"],
                app_version = release["app_version"]
            )
            for release in await self._command.search(
                search_keyword=search_keyword,
                all_versions=all_versions,
                devel=devel,
            )
        )


    async def install_or_upgrade_release(
        self,
        release_name: str,
        chart: Chart,
        *values: t.Dict[str, t.Any],
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
                mergeconcat(*values) if values else None,
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

    async def get_proceedable_revision(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None,
        timeout: t.Union[int, str, None] = None
    ) -> ReleaseRevision:
        """
        Returns a proceedable revision for the named release by rolling back or deleting
        as appropriate where the release has been left in a pending state.
        """
        try:
            current_revision = await self.get_current_revision(
                release_name,
                namespace = namespace
            )
        except ReleaseNotFoundError:
            # This condition is an easy one ;-)
            return None
        else:
            if current_revision.status in {
                # If the release is stuck in pending-install, there is nothing to rollback to
                # Instead, we have to uninstall the release and try again
                ReleaseRevisionStatus.PENDING_INSTALL,
                # If the release is stuck in uninstalling, we need to complete the uninstall
                ReleaseRevisionStatus.UNINSTALLING,
            }:
                await current_revision.release.uninstall(timeout = timeout, wait = True)
                return None
            elif current_revision.status in {
                # If the release is stuck in pending-upgrade, we need to rollback to the previous
                # revision before trying the upgrade again
                ReleaseRevisionStatus.PENDING_UPGRADE,
                # For a release stuck in pending-rollback, we need to complete the rollback
                ReleaseRevisionStatus.PENDING_ROLLBACK,
            }:
                return await current_revision.release.rollback(
                    cleanup_on_fail = True,
                    timeout = timeout,
                    wait = True
                )
            else:
                # All other statuses are proceedable
                return current_revision

    async def should_install_or_upgrade_release(
        self,
        current_revision: t.Optional[ReleaseRevision],
        chart: Chart,
        *values: t.Dict[str, t.Any]
    ) -> bool:
        """
        Returns True if an install or upgrade is required based on the given revision,
        chart and values, False otherwise.
        """
        values = mergeconcat(*values) if values else {}
        if current_revision:
            # If the current revision was not deployed successfully, always redeploy
            if current_revision.status != ReleaseRevisionStatus.DEPLOYED:
                return True
            # If the chart has changed from the deployed release, we should redeploy
            revision_chart = await current_revision.chart_metadata()
            if revision_chart.name != chart.metadata.name:
                return True
            if revision_chart.version != chart.metadata.version:
                return True
            # If the values have changed from the deployed release, we should redeploy
            revision_values = await current_revision.values()
            if revision_values != values:
                return True
            # If the chart and values are the same, there is nothing to do
            return False
        else:
            # No current revision - install is always required
            return True

    async def ensure_release(
        self,
        release_name: str,
        chart: Chart,
        *values: t.Dict[str, t.Any],
        atomic: bool = False,
        cleanup_on_fail: bool = False,
        create_namespace: bool = True,
        description: t.Optional[str] = None,
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
        Ensures the named release matches the given chart and values and return the current
        revision.

        It the release must be rolled back or deleted in order to be proceedable, this method
        will ensure that happens. It will also only make a new release if the chart and/or
        values have changed.
        """
        values = mergeconcat(*values) if values else {}
        current_revision = await self.get_proceedable_revision(
            release_name,
            namespace = namespace,
            timeout = timeout
        )
        should_install_or_upgrade = await self.should_install_or_upgrade_release(
            current_revision,
            chart,
            values
        )
        if should_install_or_upgrade:
            return await self.install_or_upgrade_release(
                release_name,
                chart,
                values,
                atomic = atomic,
                cleanup_on_fail = cleanup_on_fail,
                create_namespace = create_namespace,
                description = description,
                force = force,
                namespace = namespace,
                no_hooks = no_hooks,
                reset_values = reset_values,
                reuse_values = reuse_values,
                skip_crds = skip_crds,
                timeout = timeout,
                wait = wait
            )
        else:
            return current_revision

    async def uninstall_release(
        self,
        release_name: str,
        *,
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
        try:
            await self._command.uninstall(
                release_name,
                dry_run = dry_run,
                keep_history = keep_history,
                namespace = namespace,
                no_hooks = no_hooks,
                timeout = timeout,
                wait = wait
            )
        except ReleaseNotFoundError:
            # If the release does not exist, it is deleted :-)
            pass
