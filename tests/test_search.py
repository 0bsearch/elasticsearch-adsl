from elasticsearch_dsl.response import Hit, Response
from pytest import fixture

from elasticsearch_adsl.connections import create_connection
from elasticsearch_adsl.search import Search


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


@fixture(autouse=True)
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


async def test_count(index_name):
    search = Search(index=index_name)
    assert await search.count() == 3


async def test_count_query(index_name):
    search = Search(index=index_name).query('range', value={'gt': 1})
    assert await search.count() == 2


async def test_count_prefetch(aes, mocker):  # FIXME: use high-level interface
    mocker.spy(aes, 'count')

    search = Search()
    await search.execute()

    assert await search.count() == 3
    assert aes.count.call_count == 0


async def test_execute(index_name):
    result = await Search(index=index_name).execute()
    assert isinstance(result, Response)
    assert len(result.hits) == 3


async def test_execute_query(index_name):
    result = await Search(index=index_name).query('term', value=1).execute()
    assert result.hits[0] is result[0]
    hit = result[0]
    assert isinstance(hit, Hit)
    assert hit.value == 1


async def test_execute_cache(aes, index_name, mocker):
    mocker.spy(aes, 'search')
    search = Search(index=index_name)

    result1 = await search.execute()
    result2 = await search.execute()
    assert result1.hits == result2.hits
    assert aes.search.call_count == 1

    result3 = await search.execute(ignore_cache=True)
    assert result3.hits == result1.hits
    assert aes.search.call_count == 2
