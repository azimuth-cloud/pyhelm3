from unittest.mock import AsyncMock, Mock

import pytest
import semver

from pyhelm3 import Client, ReleaseRevision
from pyhelm3.models import ReleaseRevisionStatus

"""
There are many arguments that can be passed to client methods that are processed and
then passed through to `client.command` methods and then passed to a CLI command via
subprocess.
In order to test the processing and logic which may override or drop arguments we need
to retrieve the arguments passed to `command.run`. This requires mocking the actual run
command and also the intermediate ReleaseRevision cast. The mocks need to be applied
after loading a test chart.
"""

original_from_status = ReleaseRevision._from_status


@pytest.mark.asyncio
async def test_server_side_v4():
    """Assert that when using a Helm v4 client and passing server-side as an
    argument to the client it is used in the CLI command correctly
    """
    helm_client = Client()
    helm_client.version = semver.VersionInfo.parse("4.1.0")

    chart = await helm_client.get_chart(
        chart_ref="oci://registry-1.docker.io/bitnamicharts/etcd",
    )

    helm_client._command.run = AsyncMock(return_value=b"{}")
    ReleaseRevision._from_status = Mock(return_value="a")

    await helm_client.install_or_upgrade_release(
        release_name="test-release", chart=chart, server_side="true"
    )

    final_helm_command = helm_client._command.run.call_args[0]
    assert "--server-side=true" in final_helm_command[0]


@pytest.mark.asyncio
async def test_server_side_v3():
    """Assert that when using a Helm v3 client and passing server-side as an
    argument to the client the argument is dropped and a warning posted.
    """
    helm_client = Client()
    helm_client.version = semver.VersionInfo.parse("3.5.0")

    chart = await helm_client.get_chart(
        chart_ref="oci://registry-1.docker.io/bitnamicharts/etcd",
    )

    helm_client._command.run = AsyncMock(return_value=b"{}")
    ReleaseRevision._from_status = Mock(return_value="a")

    with pytest.warns(
        UserWarning,
        match="helm install|upgrade: Argument --server-side is undefined in helm v3,"
        "dropping it",
    ):
        await helm_client.install_or_upgrade_release(
            release_name="test-release", chart=chart, server_side="true"
        )

    final_helm_command = helm_client._command.run.call_args[0]
    assert not any([str(x).startswith("--server-side") for x in final_helm_command[0]])


@pytest.mark.asyncio
async def test_helm_v421_or_later_oci_registry_details_on_stdout():
    helm_client = Client()
    helm_client.version = semver.VersionInfo.parse("4.3.0")

    chart = await helm_client.get_chart(
        chart_ref="oci://registry-1.docker.io/bitnamicharts/etcd",
    )

    helm_install_output = b"""
Pulled: registry-1.docker.io/bitnamicharts/etcd:12.0.18
Digest: sha256:e65d94470c62e6715101eb05f865e07cc25af790bbabac3e13a0a9df51eca405
{"name":"test-release","namespace":"default","version":1,"info":{"status":"deployed",
"last_deployed":"2026-09-15T15:23:44.006824661+01:00"},"chart":{"metadata":
{"apiVersion":"v2","name":"etcd","version":"12.0.18"}},"manifest":""}
"""

    helm_client._command.run = AsyncMock(return_value=helm_install_output)
    ReleaseRevision._from_status = original_from_status

    revision = await helm_client.install_or_upgrade_release(
        release_name="test-release", chart=chart
    )
    assert revision.status == ReleaseRevisionStatus.DEPLOYED
