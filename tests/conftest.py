from _pytest.fixtures import fixture

from elasticsearch_adsl.connections import create_connection


@fixture
def index_name():
    return 'test_elasticsearch_adsl'


@fixture
async def aes(index_name, loop):
    aes = create_connection()
    yield aes
    await aes.delete_by_query(
        index=index_name,
        body={'query': {'match_all': {}}},
        refresh=True
    )
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
