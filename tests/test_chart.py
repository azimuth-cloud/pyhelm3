from pyhelm3 import Client

import pytest

@pytest.mark.asyncio
async def test_oci_chart():
    helm_client = Client()
    chart = await helm_client.get_chart(
        chart_ref='oci://registry-1.docker.io/bitnamicharts/etcd',
    )

    assert chart.metadata.name == 'etcd'
