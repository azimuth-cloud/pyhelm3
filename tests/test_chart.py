import pathlib

import pytest
from pydantic import AnyUrl, HttpUrl

from pyhelm3 import Client


@pytest.mark.asyncio
async def test_oci_chart():
    helm_client = Client()
    chart = await helm_client.get_chart(
        chart_ref="oci://registry-1.docker.io/bitnamicharts/etcd",
    )

    assert chart.metadata.name == "etcd"


@pytest.mark.asyncio
async def test_http_chart():
    helm_client = Client()
    chart = await helm_client.get_chart(
        chart_ref="https://github.com/prometheus-community/helm-charts/releases/download/kube-prometheus-stack-87.20.0/kube-prometheus-stack-87.20.0.tgz",
    )

    # Check the chart is loaded correctly and metadata parsed
    assert chart.metadata.name == "kube-prometheus-stack"
    readme = await chart.readme()
    assert isinstance(readme, str)
    assert isinstance(chart.ref, HttpUrl)


@pytest.mark.asyncio
async def test_local_chart():
    helm_client = Client()
    chart = await helm_client.get_chart(
        chart_ref=pathlib.Path.cwd() / "tests/test-chart",
    )
    # Check the chart is loaded correctly and icon url parsed
    assert chart.metadata.name == "test-chart"
    assert isinstance(chart.metadata.icon, AnyUrl)
    assert str(chart.metadata.icon) == "data://notreal"
