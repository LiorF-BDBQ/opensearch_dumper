from pathlib import Path
from time import sleep, time

import pytest
from click.testing import CliRunner
from docker.types import Ulimit
from opensearchpy import OpenSearch, NotFoundError
from testcontainers.general import DockerContainer

from main import dump
from utils import dump_index, ingest, split_file_by_rows, process_chunks, PROCESS_START

OS_PORT = 9200
CONTAINER_START_TIMEOUT = 10


@pytest.fixture(scope="session")
def opensearch():
    container = (
        DockerContainer(image="opensearchproject/opensearch:2.3.0")
        .with_env("discovery.type", "single-node")
        .with_env("node.name", "opensearch-node")
        .with_env("cluster.name", "opensearch-cluster")
        .with_env("DISABLE_SECURITY_PLUGIN", "true")
        .with_env("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_bind_ports(9200, 9200)
        .with_bind_ports(9600, 9600)
        .with_kwargs(
            network="opensearch-net",
            ulimits=[
                Ulimit(name="memlock", soft=-1, hard=-1),
                Ulimit(name="nofile", soft=65536, hard=65536),
            ],
        )
        .with_name("os")
    )
    docker_client = container.get_docker_client()
    if "opensearch-net" not in [
        n.attrs.get("Name") for n in docker_client.client.networks.list()
    ]:
        docker_client.client.networks.create("opensearch-net")
    container.start()
    client = OpenSearch(hosts="http://localhost:9200")
    start_time = time()
    while time() - start_time < CONTAINER_START_TIMEOUT:
        # noinspection PyBroadException
        try:
            assert client.cluster.health(wait_for_status="green")["status"] == "green"
            break
        except Exception:
            sleep(2)
    assert client.cluster.health(wait_for_status="green")["status"] == "green"
    yield client
    container.stop()


def test_connection(opensearch: OpenSearch):
    assert opensearch.ping()


def test_ingest_file_and_dump(opensearch: OpenSearch):
    try:
        opensearch.indices.delete(__name__)
    except NotFoundError:
        pass
    opensearch.indices.create(__name__)
    chunk_size = 100 * 1000 * 1000
    test_index_name = "test-100k"
    ingest(
        opensearch,
        "./__fixtures__/100k.jsonl",
        True,
        60,
        100,
        2,
        test_index_name,
        chunk_size,
    )
    result = dump_index(opensearch, test_index_name, 2, 10000, 60)
    assert result


def test_ingest(opensearch: OpenSearch):
    test_index_name = "test-100k-dumped"
    try:
        opensearch.indices.delete(test_index_name)
    except NotFoundError:
        pass
    opensearch.indices.create(test_index_name)
    chunk_size = 100 * 1000 * 1000
    result = ingest(
        opensearch,
        "./__fixtures__/100k_dumped.jsonl",
        True,
        60,
        100,
        2,
        test_index_name,
        chunk_size,
        use_retry_mechanism=True
    )
    assert result


def test_index_doesnt_exist(opensearch: OpenSearch):
    runner = CliRunner()
    result = runner.invoke(dump, ["crap"])
    assert result.exit_code == 1
    assert isinstance(result.exception, NotFoundError)


def test_e2e(opensearch: OpenSearch):
    pass


def test_get_offsets():
    row_count, offsets = split_file_by_rows("./__fixtures__/100k.jsonl", 2)
    assert row_count == 100000 and len(offsets) == 2
    read_actions = 0
    for offset in offsets:
        for chunk in process_chunks(
            offset, "./__fixtures__/100k.jsonl", 10000, True, "test"
        ):
            read_actions += len(chunk)
    assert read_actions == 100000
