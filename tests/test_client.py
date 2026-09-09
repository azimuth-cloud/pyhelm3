from unittest.mock import AsyncMock, Mock

import pytest
import semver

from pyhelm3 import Client, ReleaseRevision

"""
There are many arguments that can be passed to client methods that are processed and
then passed through to `client.command` methods and then passed to a CLI command via
subprocess.
In order to test the processing and logic which may override or drop arguments we need
to retrieve the arguments passed to `command.run`. This requires mocking the actual run
command and also the intermediate ReleaseRevision cast. The mocks need to be applied
after loading a test chart.
"""


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
