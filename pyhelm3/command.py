import asyncio
import json
import logging
import pathlib
import re
import shlex
import shutil
import tempfile
import typing as t

import yaml

from . import errors


class SafeLoader(yaml.SafeLoader):
    """
    We use a custom YAML loader that doesn't bork on plain equals '=' signs.
    
    It was originally designated with a special meaning, but noone uses it:

        https://github.com/yaml/pyyaml/issues/89
        https://yaml.org/type/value.html
    """
    @staticmethod
    def construct_value(loader, node):
        return loader.construct_scalar(node)

SafeLoader.add_constructor("tag:yaml.org,2002:value", SafeLoader.construct_value)


CHART_METADATA_TEMPLATE = """
{{- with .Release.Chart.Metadata }}
apiVersion: {{ .APIVersion }}
name: {{ printf "%q" .Name }}
version: {{ printf "%q" .Version }}
{{- with .KubeVersion }}
kubeVersion: {{ printf "%q" . }}
{{- end }}
{{- with .Description }}
description: {{ printf "%q" . }}
{{- end }}
{{- with .Type }}
type: {{ printf "%q" . }}
{{- end }}
{{- with .Keywords }}
keywords:
  {{- range . }}
  - {{ printf "%q" . }}
  {{- end }}
{{- end }}
{{- with .Home }}
home: {{ printf "%q" . }}
{{- end }}
{{- with .Sources }}
sources:
  {{- range . }}
  - {{ printf "%q" . }}
  {{- end }}
{{- end }}
{{- with .Dependencies }}
dependencies:
  {{- range . }}
  - name: {{ printf "%q" .Name }}
    version: {{ printf "%q" .Version }}
    {{- with .Repository }}
    repository: {{ printf "%q" . }}
    {{- end }}
    {{- with .Condition }}
    condition: {{ printf "%q" . }}
    {{- end }}
    {{- with .Tags }}
    tags:
      {{- range . }}
      - {{ printf "%q" . }}
      {{- end }}
    {{- end }}
    {{- with .ImportValues }}
    import-values:
      {{- range . }}
      {{- if eq "string" (printf "%T" .) }}
      - {{ printf "%q" . }}
      {{- else }}
      -
        {{- range $k, $v := . }}
        {{ $k }}: {{ printf "%q" $v }}
        {{- end }}
      {{- end }}
      {{- end }}
    {{- end }}
    {{- with .Alias }}
    alias: {{ printf "%q" . }}
    {{- end }}
  {{- end }}
{{- end }}
{{- with .Maintainers }}
maintainers:
  {{- range . }}
  - name: {{ printf "%q" .Name }}
    {{- with .Email }}
    email: {{ printf "%q" . }}
    {{- end }}
    {{- with .URL }}
    url: {{ printf "%q" . }}
    {{- end }}
  {{- end }}
{{- end }}
{{- with .Icon }}
icon: {{ printf "%q" . }}
{{- end }}
{{- with .AppVersion }}
appVersion: {{ printf "%q" . }}
{{- end }}
{{- if .Deprecated }}
deprecated: true
{{- end }}
{{- with .Annotations }}
annotations:
  {{- range $k, $v := . }}
  {{ $k }}: {{ printf "%q" $v }}
  {{- end }}
{{- end }}
{{- end }}
"""


#: Bound type var for forward references
CommandType = t.TypeVar("CommandType", bound = "Command")


CHART_NOT_FOUND = re.compile(r"chart \"[^\"]+\" (version \"[^\"]+\" )?not found")
CONNECTION_ERROR = re.compile(r"(read: operation timed out|connect: network is unreachable)")


class Command:
    """
    Class presenting an async interface around the Helm CLI.
    """
    def __init__(
        self,
        *,
        default_timeout: t.Union[int, str] = "5m",
        executable: str = "helm",
        history_max_revisions: int = 10,
        insecure_skip_tls_verify: bool = False,
        kubeconfig: t.Optional[pathlib.Path] = None,
        kubecontext: t.Optional[str] = None,
        unpack_directory: t.Optional[str] = None
    ):
        self._logger = logging.getLogger(__name__)
        self._default_timeout = default_timeout
        self._executable = executable
        self._history_max_revisions = history_max_revisions
        self._insecure_skip_tls_verify = insecure_skip_tls_verify
        self._kubeconfig = kubeconfig
        self._kubecontext = kubecontext
        self._unpack_directory = unpack_directory

    def _log_format(self, argument):
        argument = str(argument)
        if argument == "-":
            return "<stdin>"
        elif "\n" in argument:
            return "<multi-line string>"
        else:
            return argument

    async def run(self, command: t.List[str], input: t.Optional[bytes] = None) -> bytes:
        """
        Run the given Helm command with the given input as stdin and 
        """
        command = [self._executable] + command
        if self._kubeconfig:
            command.extend(["--kubeconfig", self._kubeconfig])
        if self._kubecontext:
            command.extend(["--kube-context", self._kubecontext])
        # The command must be made up of str and bytes, so convert anything that isn't
        shell_formatted_command = shlex.join(
            part if isinstance(part, (str, bytes)) else str(part)
            for part in command
        )
        log_formatted_command = shlex.join(self._log_format(part) for part in command)
        self._logger.info("running command: %s", log_formatted_command)
        proc = await asyncio.create_subprocess_shell(
            shell_formatted_command,
            # Only make stdin a pipe if we have input to feed it
            stdin = asyncio.subprocess.PIPE if input is not None else None,
            stdout = asyncio.subprocess.PIPE,
            stderr = asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await proc.communicate(input)
        except asyncio.CancelledError:
            # If the asyncio task is cancelled, terminate the Helm process but let the
            # process handle the termination and exit
            # We occassionally see a ProcessLookupError here if the process finished between
            # us being cancelled and terminating the process, which we ignore as that is our
            # target state anyway
            try:
                proc.terminate()
                _ = await proc.communicate()
            except ProcessLookupError:
                pass
            # Once the process has exited, re-raise the cancelled error
            raise
        if proc.returncode == 0:
            self._logger.info("command succeeded: %s", log_formatted_command)
            return stdout
        else:
            self._logger.warning("command failed: %s", log_formatted_command)
            stderr_str = stderr.decode().lower()
            # Parse some expected errors into specific exceptions
            if "context canceled" in stderr_str:
                error_cls = errors.CommandCancelledError
            # Any error referencing etcd is a connection error
            # This must be before other rules, as it sometimes occurs alonside a not found error
            elif "etcdserver" in stderr_str:
                error_cls = errors.ConnectionError
            elif "release: not found" in stderr_str:
                error_cls = errors.ReleaseNotFoundError
            elif "failed to render chart" in stderr_str:
                error_cls = errors.FailedToRenderChartError
            elif "execution error" in stderr_str:
                error_cls = errors.FailedToRenderChartError
            elif "rendered manifests contain a resource that already exists" in stderr_str:
                error_cls = errors.ResourceAlreadyExistsError
            elif "is invalid" in stderr_str:
                error_cls = errors.InvalidResourceError
            elif CHART_NOT_FOUND.search(stderr_str) is not None:
                error_cls = errors.ChartNotFoundError
            elif CONNECTION_ERROR.search(stderr_str) is not None:
                error_cls = errors.ConnectionError
            else:
                error_cls = errors.Error
            raise error_cls(proc.returncode, stdout, stderr)

    async def diff_release(
        self,
        release_name: str,
        other_release_name: str,
        *,
        # The number of lines of context to show around each diff
        context_lines: t.Optional[int] = None,
        namespace: t.Optional[str] = None,
        # Indicates whether to show secret values in the diff
        show_secrets: bool = True
    ) -> str:
        """
        Returns the diff between two releases created from the same chart.
        """
        command = [
            "diff",
            "release",
            release_name,
            other_release_name,
            "--no-color",
            "--normalize-manifests",
        ]
        if context_lines is not None:
            command.extend(["--context", context_lines])
        if namespace:
            command.extend(["--namespace", namespace])
        if show_secrets:
            command.append("--show-secrets")
        return (await self.run(command)).decode()

    async def diff_revision(
        self,
        release_name: str,
        revision: int,
        # If not specified, the diff is with latest
        other_revision: t.Optional[int] = None,
        *,
        # The number of lines of context to show around each diff
        context_lines: t.Optional[int] = None,
        namespace: t.Optional[str] = None,
        # Indicates whether to show secret values in the diff
        show_secrets: bool = True
    ) -> str:
        """
        Returns the diff between two revisions of the specified release.

        If the second revision is not specified, the latest revision is used.
        """
        command = [
            "diff",
            "revision",
            release_name,
            revision,
        ]
        if other_revision is not None:
            command.append(other_revision)
        command.extend(["--no-color", "--normalize-manifests"])
        if context_lines is not None:
            command.extend(["--context", context_lines])
        if namespace:
            command.extend(["--namespace", namespace])
        if show_secrets:
            command.append("--show-secrets")
        return (await self.run(command)).decode()

    async def diff_rollback(
        self,
        release_name: str,
        # The revision to simulate rolling back to
        revision: t.Optional[int] = None,
        *,
        # The number of lines of context to show around each diff
        context_lines: t.Optional[int] = None,
        namespace: t.Optional[str] = None,
        # Indicates whether to show secret values in the diff
        show_secrets: bool = True
    ) -> str:
        """
        Returns the diff that would result from rolling back the given release
        to the specified revision.
        """
        command = [
            "diff",
            "rollback",
            release_name,
        ]
        if revision is not None:
            command.append(revision)
        command.extend(["--no-color", "--normalize-manifests"])
        if context_lines is not None:
            command.extend(["--context", context_lines])
        if namespace:
            command.extend(["--namespace", namespace])
        if show_secrets:
            command.append("--show-secrets")
        return (await self.run(command)).decode()

    async def diff_upgrade(
        self,
        release_name: str,
        chart_ref: t.Union[pathlib.Path, str],
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        # The number of lines of context to show around each diff
        context_lines: t.Optional[int] = None,
        devel: bool = False,
        dry_run: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        repo: t.Optional[str] = None,
        reset_values: bool = False,
        reuse_values: bool = False,
        # Indicates whether to show secret values in the diff
        show_secrets: bool = True,
        version: t.Optional[str] = None
    ) -> str:
        """
        Returns the diff that would result from rolling back the given release
        to the specified revision.
        """
        command = [
            "diff",
            "upgrade",
            release_name,
            chart_ref,
            "--allow-unreleased",
            "--no-color",
            "--normalize-manifests",
            # Disable OpenAPI validation as we still want the diff to work when CRDs change
            "--disable-openapi-validation",
            # We pass the values using stdin
            "--values", "-",
        ]
        if context_lines is not None:
            command.extend(["--context", context_lines])
        if devel:
            command.append("--devel")
        if dry_run:
            command.append("--dry-run")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if repo:
            command.extend(["--repo", repo])
        if reset_values:
            command.append("--reset-values")
        if reuse_values:
            command.append("--reuse-values")
        if show_secrets:
            command.append("--show-secrets")
        if version:
            command.extend(["--version", version])
        return (await self.run(command, json.dumps(values or {}).encode())).decode()

    async def diff_version(self) -> str:
        """
        Returns the version of the Helm diff plugin (https://github.com/databus23/helm-diff).
        """
        return (await self.run(["diff", "version"])).decode()

    async def get_chart_metadata(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None,
        revision: t.Optional[int] = None
    ):
        """
        Returns metadata for the chart that was used to deploy the release.
        """
        # There is no native command for this (!!!!) so use the templating
        # functionality to template out some YAML
        command = [
            "get",
            "all",
            release_name,
            # Use the chart metadata template
            "--template", CHART_METADATA_TEMPLATE
        ]
        if namespace:
            command.extend(["--namespace", namespace])
        if revision is not None:
            command.extend(["--revision", revision])
        return yaml.load(await self.run(command), Loader = SafeLoader)

    async def get_hooks(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None,
        revision: t.Optional[int] = None
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the hooks for the specified release.
        """
        command = ["get", "hooks", release_name]
        if revision is not None:
            command.extend(["--revision", revision])
        if namespace:
            command.extend(["--namespace", namespace])
        return yaml.load_all(await self.run(command), Loader = SafeLoader)

    async def get_resources(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None,
        revision: t.Optional[int] = None
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the resources for the specified release.
        """
        command = ["get", "manifest", release_name]
        if revision is not None:
            command.extend(["--revision", revision])
        if namespace:
            command.extend(["--namespace", namespace])
        return yaml.load_all(await self.run(command), Loader = SafeLoader)

    async def get_values(
        self,
        release_name: str,
        *,
        computed: bool = False,
        namespace: t.Optional[str] = None,
        revision: t.Optional[int] = None
     ) -> t.Dict[str, t.Any]:
        """
        Returns the values for the specified release.

        Optionally, the full computed values can be requested.
        """
        command = ["get", "values", release_name, "--output", "json"]
        if computed:
            command.append("--all")
        if revision is not None:
            command.extend(["--revision", revision])
        if namespace:
            command.extend(["--namespace", namespace])
        return json.loads(await self.run(command)) or {}

    async def history(
        self,
        release_name: str,
        *,
        max_revisions: int = 256,
        namespace: t.Optional[str] = None
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the historical revisions for the specified release.

        The maximum number of revisions to return can be specified (defaults to 256).
        """
        command = ["history", release_name, "--output", "json", "--max", max_revisions]
        if namespace:
            command.extend(["--namespace", namespace])
        return json.loads(await self.run(command))

    async def install_or_upgrade(
        self,
        release_name: str,
        chart_ref: t.Union[pathlib.Path, str],
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        atomic: bool = False,
        cleanup_on_fail: bool = False,
        create_namespace: bool = True,
        description: t.Optional[str] = None,
        devel: bool = False,
        dry_run: bool = False,
        force: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        repo: t.Optional[str] = None,
        reset_values: bool = False,
        reuse_values: bool = False,
        skip_crds: bool = False,
        timeout: t.Union[int, str, None] = None,
        version: t.Optional[str] = None,
        wait: bool = False
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Installs or upgrades the specified release using the given chart and values.
        """
        command = [
            "upgrade",
            release_name,
            chart_ref,
            "--history-max", self._history_max_revisions,
            "--install",
            "--output", "json",
            # Use the default timeout unless an override is specified
            "--timeout", timeout if timeout is not None else self._default_timeout,
            # We send the values in on stdin
            "--values", "-",
        ]
        if atomic:
            command.append("--atomic")
        if cleanup_on_fail:
            command.append("--cleanup-on-fail")
        if create_namespace:
            command.append("--create-namespace")
        if description:
            command.extend(["--description", description])
        if devel:
            command.append("--devel")
        if dry_run:
            command.append("--dry-run")
        if force:
            command.append("--force")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if repo:
            command.extend(["--repo", repo])
        if reset_values:
            command.append("--reset-values")
        if reuse_values:
            command.append("--reuse-values")
        if skip_crds:
            command.append("--skip-crds")
        if version:
            command.extend(["--version", version])
        if wait:
            command.extend(["--wait", "--wait-for-jobs"])
        return json.loads(await self.run(command, json.dumps(values or {}).encode()))

    async def list(
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
    ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the list of releases that match the given options.
        """
        command = ["list", "--max", max_releases, "--output", "json"]
        if all:
            command.append("--all")
        if all_namespaces:
            command.append("--all-namespaces")
        if include_deployed:
            command.append("--deployed")
        if include_failed:
            command.append("--failed")
        if include_pending:
            command.append("--pending")
        if include_superseded:
            command.append("--superseded")
        if include_uninstalled:
            command.append("--uninstalled")
        if include_uninstalling:
            command.append("--uninstalling")
        if namespace:
            command.extend(["--namespace", namespace])
        if sort_by_date:
            command.append("--date")
        if sort_reversed:
            command.append("--reverse")
        return json.loads(await self.run(command))

    async def pull(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> pathlib.Path:
        """
        Fetch a chart from a remote location and unpack it locally.

        Returns the path of the directory into which the chart was downloaded and unpacked.
        """
        # Make a directory to unpack into
        destination = tempfile.mkdtemp(prefix = "helm.", dir = self._unpack_directory)
        command = ["pull", chart_ref, "--destination", destination, "--untar"]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        await self.run(command)
        return pathlib.Path(destination).resolve()

    async def repo_list(self) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Lists the available Helm repositories.
        """
        return json.loads(await self.run(["repo", "list", "--output", "json"]))

    async def repo_add(self, name: str, url: str):
        """
        Adds a repository to the available Helm repositories.

        Returns the new repo list on success.
        """
        command = ["repo", "add", name, url, "--force-update"]
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        await self.run(command)

    async def repo_update(self, *names: str):
        """
        Updates the chart indexes for the specified repositories.

        If no repositories are given, all repositories are updated.

        Returns the repo list on success.
        """
        await self.run(["repo", "update", "--fail-on-repo-update-fail"] + list(names))

    async def repo_remove(self, name: str):
        """
        Removes the specified chart.

        Returns the new repo list on success.
        """
        try:
            await self.run(["repo", "remove", name])
        except errors.Error as exc:
            if "no repo named" not in exc.stderr.decode().lower():
                raise

    async def rollback(
        self,
        release_name: str,
        revision: t.Optional[int],
        *,
        cleanup_on_fail: bool = False,
        dry_run: bool = False,
        force: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        recreate_pods: bool = False,
        timeout: t.Union[int, str, None] = None,
        wait: bool = False
    ):
        """
        Rollback the specified release to the specified revision.
        """
        command = [
            "rollback",
            release_name,
        ]
        if revision is not None:
            command.append(revision)
        command.extend([
            "--history-max", self._history_max_revisions,
            # Use the default timeout unless an override is specified
            "--timeout", timeout if timeout is not None else self._default_timeout,
        ])
        if cleanup_on_fail:
            command.append("--cleanup-on-fail")
        if dry_run:
            command.append("--dry-run")
        if force:
            command.append("--force")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if recreate_pods:
            command.append("--recreate-pods")
        if wait:
            command.extend(["--wait", "--wait-for-jobs"])
        await self.run(command)

    async def search(
        self,
        search_keyword: t.Optional[str] = None,
        *,
        all_versions: bool = False,
        devel: bool = False,
        version_constraints: t.Optional[str] = None
    ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Search the available Helm repositories for charts matching the specified constraints.
        """
        command = ["search", "repo", "--output", "json"]
        if search_keyword:
            command.append(search_keyword)
        if all_versions:
            command.append("--versions")
        if devel:
            command.append("--devel")
        if version_constraints:
            command.extend(["--version", version_constraints])
        return json.loads(await self.run(command))

    async def show_chart(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        """
        Returns the contents of Chart.yaml for the specified chart.
        """
        command = ["show", "chart", chart_ref]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        return yaml.load(await self.run(command), Loader = SafeLoader)

    async def show_crds(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the CRDs for the specified chart.
        """
        # Until https://github.com/helm/helm/issues/11261 is fixed, we must manually
        # unpack the chart and parse the files in the ./crds directory ourselves
        # This is what the implementation should be
        # command = ["show", "crds", chart_ref]
        # if devel:
        #     command.append("--devel")
        # if self._insecure_skip_tls_verify:
        #     command.append("--insecure-skip-tls-verify")
        # if repo:
        #     command.extend(["--repo", repo])
        # if version:
        #     command.extend(["--version", version])
        # return return yaml.load_all(await self.run(command), Loader = SafeLoader)

        # If ephemeral_path is set, it will be deleted at the end of the method
        ephemeral_path = None
        try:
            if repo:
                # If a repo is given, assume that the chart ref is a chart name in that repo
                ephemeral_path = await self.pull(
                    chart_ref,
                    devel = devel,
                    repo = repo,
                    version = version
                )
                chart_directory = next(ephemeral_path.glob("**/Chart.yaml")).parent
            else:
                # If not, we have either a path (directory or archive) or a URL to a chart
                try:
                    chart_path = pathlib.Path(chart_ref).resolve(strict = True)
                except (TypeError, ValueError, FileNotFoundError):
                    # Assume we have a URL that needs pulling
                    ephemeral_path = await self.pull(chart_ref)
                    chart_directory = next(ephemeral_path.glob("**/Chart.yaml")).parent
                else:
                    if chart_path.is_dir():
                        # Just make sure that the directory is a chart
                        chart_directory = next(chart_path.glob("**/Chart.yaml")).parent
                    else:
                        raise RuntimeError("local archive files are not currently supported")
            def yaml_load_all(file):
                with file.open() as fh:
                    yield from yaml.load_all(fh, Loader = SafeLoader)
            return [
                crd
                for crd_file in chart_directory.glob("crds/**/*.yaml")
                for crd in yaml_load_all(crd_file)
            ]
        finally:
            if ephemeral_path and ephemeral_path.is_dir():
                shutil.rmtree(ephemeral_path)

    async def show_readme(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> str:
        """
        Returns the README for the specified chart.
        """
        command = ["show", "readme", chart_ref]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        return (await self.run(command)).decode()

    async def show_values(
        self,
        chart_ref: t.Union[pathlib.Path, str],
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> t.Dict[str, t.Any]:
        """
        Returns the default values for the specified chart.
        """
        command = ["show", "values", chart_ref]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        return yaml.load(await self.run(command), Loader = SafeLoader)

    async def status(
        self,
        release_name: str,
        *,
        namespace: t.Optional[str] = None,
        revision: t.Optional[int] = None,
    ):
        """
        Get the status of the specified release.
        """
        command = ["status", release_name, "--output", "json"]
        if namespace:
            command.extend(["--namespace", namespace])
        if revision:
            command.extend(["--revision", revision])
        return json.loads(await self.run(command))

    async def template(
        self,
        release_name: str,
        chart_ref: t.Union[pathlib.Path, str],
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        devel: bool = False,
        include_crds: bool = False,
        is_upgrade: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None,
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Renders the chart templates and returns the resources.
        """
        command = [
            "template",
            release_name,
            chart_ref,
            "--include-crds" if include_crds else "--skip-crds",
            # We send the values in on stdin
            "--values", "-",
        ]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if is_upgrade:
            command.append("--is-upgrade")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        return yaml.load_all(
            await self.run(command, json.dumps(values or {}).encode()),
            Loader = SafeLoader
        )

    async def uninstall(
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
        Uninstall the specified release.
        """
        command = [
            "uninstall",
            release_name,
            # Use the default timeout unless an override is specified
            "--timeout", timeout if timeout is not None else self._default_timeout,
        ]
        if dry_run:
            command.append("--dry-run")
        if keep_history:
            command.append("--keep-history")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if wait:
            command.extend(["--wait"])
        await self.run(command)

    async def version(self) -> str:
        """
        Returns the Helm version.
        """
        return (await self.run(["version", "--template", "{{ .Version }}"])).decode()
