from _pytest.fixtures import fixture
from elasticsearch_dsl import connections

from elasticsearch_adsl.connections import create_connection


@fixture
def index_name():
    return 'test_elasticsearch_adsl'


@fixture
async def aes(index_name, loop):
    aes = create_connection()
    yield aes
    await aes.indices.delete(index_name, ignore=404)
    await aes.transport.close()


@fixture
async def test_data(aes, index_name):
    await aes.bulk(
        body=[
            {'index': {}}, {'value': 1},
            {'index': {}}, {'value': 2},
            {'index': {}}, {'value': 3},
        ],
        index=index_name,
        doc_type='doc',
        refresh=True
    )


@fixture
async def clean_connections(loop):
    yield
    for conn in connections.connections._conns.values():
        await conn.transport.close()
    connections.connections._kwargs.clear()
    connections.connections._conns.clear()
