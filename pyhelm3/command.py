import asyncio
import json
import pathlib
import shlex
import tempfile
import typing as t

import yaml


class Error(Exception):
    """
    Raised when an error occurs with a Helm command.
    """
    def __init__(self, returncode: int, stdout: bytes, stderr: bytes):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(stderr.decode())


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


class Command:
    """
    Class presenting an async interface around the Helm CLI.
    """
    def __init__(
        self,
        *,
        executable: str = "helm",
        history_max_revisions: int = 10,
        insecure_skip_tls_verify: bool = False,
        kubeconfig: t.Optional[pathlib.Path] = None,
        unpack_directory: t.Optional[str] = None
    ):
        self._executable = executable
        self._history_max_revisions = history_max_revisions
        self._insecure_skip_tls_verify = insecure_skip_tls_verify
        self._kubeconfig = kubeconfig
        self._unpack_directory = unpack_directory

    def reconfigure(
        self,
        *,
        executable: t.Optional[str] = None,
        history_max_revisions: t.Optional[int] = None,
        insecure_skip_tls_verify: t.Optional[bool] = None,
        kubeconfig: t.Optional[pathlib.Path] = None,
        unpack_directory: t.Optional[str] = None
    ) -> CommandType:
        """
        Returns a new command based on this one but with the specified reconfiguration.

        In particular, the new command shares a lock with this one.
        """
        return self.__class__(
            executable = executable or self._executable,
            history_max_revisions = history_max_revisions or self._history_max_revisions,
            insecure_skip_tls_verify = (
                insecure_skip_tls_verify
                if insecure_skip_tls_verify is not None
                else self._insecure_skip_tls_verify
            ),
            kubeconfig = kubeconfig or self._kubeconfig,
            unpack_directory = unpack_directory or self._unpack_directory
        )

    async def run(self, command: t.List[str], input: t.Optional[bytes] = None) -> bytes:
        """
        Run the given Helm command with the given input as stdin and 
        """
        command = [self._executable] + command
        if self._kubeconfig:
            command.extend(["--kubeconfig", self._kubeconfig])
        proc = await asyncio.create_subprocess_shell(
            shlex.join(command),
            # Only make stdin a pipe if we have input to feed it
            stdin = asyncio.subprocess.PIPE if input is not None else None,
            stdout = asyncio.subprocess.PIPE,
            stderr = asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate(input)
        if proc.returncode == 0:
            return stdout
        else:
            raise Error(proc.returncode, stdout, stderr)

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
            command.extend(["--revision", str(revision)])
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
            command.extend(["--revision", str(revision)])
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
            command.extend(["--revision", str(revision)])
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
            command.extend(["--revision", str(revision)])
        if namespace:
            command.extend(["--namespace", namespace])
        return json.loads(await self.run(command))

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
        command = ["history", release_name, "--output", "json", "--max", str(max_revisions)]
        if namespace:
            command.extend(["--namespace", namespace])
        return json.loads(await self.run(command))

    async def install_or_upgrade(
        self,
        release_name: str,
        chart: str,
        values: t.Optional[t.Dict[str, t.Any]] = None,
        *,
        atomic: bool = False,
        cleanup_on_fail: bool = False,
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
        timeout: t.Union[int, str] = "5m",
        version: t.Optional[str] = None,
        wait: bool = False
     ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Installs or upgrades the specified release using the given chart and values.
        """
        command = [
            "upgrade",
            release_name,
            chart,
            "--create-namespace",
            "--history-max", str(self._history_max_revisions),
            "--install",
            "--output", "json",
            # We send the values in on stdin
            "--values", "-",
        ]
        if atomic:
            command.append("--atomic")
        if cleanup_on_fail:
            command.append("--cleanup-on-fail")
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
        if timeout:
            command.extend(["--timeout", str(timeout)])
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
        command = ["list", "--max", str(max_releases), "--output", "json"]
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
        chart_ref: str,
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> pathlib.Path:
        """
        Fetch a chart from a remote location and unpack it locally.

        Returns the path of the unpacked chart.
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
        # Get the parent directory of the Chart.yaml file
        return next(pathlib.Path(destination).glob("**/Chart.yaml")).parent

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
        except Error as exc:
            if "no repo named" not in exc.stderr.decode().lower():
                raise

    async def rollback(
        self,
        release_name: str,
        revision: int,
        *,
        cleanup_on_fail: bool = False,
        dry_run: bool = False,
        force: bool = False,
        namespace: t.Optional[str] = None,
        no_hooks: bool = False,
        recreate_pods: bool = False,
        timeout: t.Union[int, str] = "5m",
        wait: bool = False
    ):
        """
        Rollback the specified release to the specified revision.
        """
        command = [
            "rollback",
            release_name,
            str(revision),
            "--history-max", str(self._history_max_revisions),
        ]
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
        if timeout:
            command.extend(["--timeout", str(timeout)])
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
        chart_ref: str,
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
        chart_ref: str,
        *,
        devel: bool = False,
        repo: t.Optional[str] = None,
        version: t.Optional[str] = None
    ) -> t.Iterable[t.Dict[str, t.Any]]:
        """
        Returns the CRDs for the specified chart.
        """
        command = ["show", "crds", chart_ref]
        if devel:
            command.append("--devel")
        if self._insecure_skip_tls_verify:
            command.append("--insecure-skip-tls-verify")
        if repo:
            command.extend(["--repo", repo])
        if version:
            command.extend(["--version", version])
        return yaml.load_all(await self.run(command), Loader = SafeLoader)

    async def show_readme(
        self,
        chart_ref: str,
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
        chart_ref: str,
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
            command.extend(["--revision", str(revision)])
        return json.loads(await self.run(command))

    async def template(
        self,
        release_name: str,
        chart: str,
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
            chart,
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
        timeout: t.Union[int, str] = "5m",
        wait: bool = False
    ):
        """
        Uninstall the specified release.
        """
        command = ["uninstall", release_name]
        if dry_run:
            command.append("--dry-run")
        if keep_history:
            command.append("--keep-history")
        if namespace:
            command.extend(["--namespace", namespace])
        if no_hooks:
            command.append("--no-hooks")
        if timeout:
            command.extend(["--timeout", str(timeout)])
        if wait:
            command.extend(["--wait", "--wait-for-jobs"])
        await self.run(command)

    async def version(self) -> str:
        """
        Returns the Helm version.
        """
        return (await self.run(["version", "--template", "{{ .Version }}"])).decode()
