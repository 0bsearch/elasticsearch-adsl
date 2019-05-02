from elasticsearch_dsl.response import Hit, Response
from pytest import fixture

from elasticsearch_adsl.search import Search
from elasticsearch_dsl.query import MatchAll


@fixture(autouse=True)
async def _test_data(aes, index_name, test_data):
    return


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


async def test_delete(index_name):
    assert await Search(index=index_name).count() == 3

    await Search(index=index_name).query('term', value=1).params(refresh=True).delete()
    assert await Search(index=index_name).count() == 2

    await Search(index=index_name).query(MatchAll()).params(refresh=True).delete()
    assert await Search(index=index_name).count() == 0


async def test_scan(index_name):
    result = [h async for h in Search(index=index_name).scan()]
    assert len(result) == 3
    assert isinstance(result[0], Hit)
    assert {h.value for h in result} == {1, 2, 3}

