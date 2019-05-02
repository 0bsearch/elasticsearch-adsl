from asyncio import Future
from unittest.mock import MagicMock

from elasticsearch.helpers import ScanError
from elasticsearch_async import AsyncElasticsearch
from pytest import raises

from elasticsearch_adsl.scan import scan


async def test_scan_basic(aes, test_data, index_name):
    results = [r async for r in scan(aes, index=index_name)]
    assert len(results) == 3


async def test_preserve_order(aes, index_name):
    await aes.bulk(
        body=[
            {'index': {}}, {'value': 3},
            {'index': {}}, {'value': 2},
            {'index': {}}, {'value': 1},
            {'index': {}}, {'value': 5},
            {'index': {}}, {'value': 4},
        ],
        index=index_name,
        doc_type='doc',
        refresh=True
    )

    results = [r async for r in scan(
        aes,
        query={'sort': 'value'},
        index=index_name,
        preserve_order=True
    )]
    assert [r['_source']['value'] for r in results] == [1, 2, 3, 4, 5]


async def test_scroll_error(aes, index_name, ):
    aes = MagicMock(spec=AsyncElasticsearch)

    _search_response = Future()
    _search_response.set_result({
        '_shards': {'total': 5, 'successful': 5},
        '_scroll_id': 42,
        'hits': {'hits': [1, 2, 3]},
    })
    aes.search.return_value = _search_response

    def _scroll_response(hits):
        response = Future()
        response.set_result({
            '_shards': {'total': 5, 'successful': 4},
            '_scroll_id': 42,
            'hits': {'hits': hits},
        })
        return response
    aes.scroll.side_effect = [_scroll_response([4, 5, 6]), _scroll_response([])]

    data = [h async for h in scan(aes, raise_on_error=False, clear_scroll=False)]
    assert data == [1, 2, 3, 4, 5, 6]
    assert aes.scroll.call_count == 2

    aes.search.reset_mock()
    aes.scroll.reset_mock()
    aes.scroll.side_effect = [_scroll_response([4, 5, 6]), _scroll_response([])]
    with raises(ScanError):
        data = []
        async for h in scan(aes, raise_on_error=True, clear_scroll=False):
            data.append(h)

    assert data == [1, 2, 3]
    assert aes.scroll.call_count == 1


async def test_initial_search_error():
    aes = MagicMock(spec=AsyncElasticsearch)

    async def _response():
        return {}
    aes.search.return_value = _response()

    data = [h async for h in scan(aes)]
    assert data == []
    assert aes.scroll.call_count == 0
    assert aes.clear_scroll.call_count == 0


async def test_fast_error_route():
    aes = MagicMock(spec=AsyncElasticsearch)

    async def _failed_response():
        return {
            '_shards': {'total': 5, 'successful': 4},
            '_scroll_id': 42,
            'hits': {'hits': [1, 2, 3]},
        }
    aes.search.return_value = _failed_response()

    async def _clean_response():
        return
    aes.clear_scroll.return_value = _clean_response()

    with raises(ScanError):
        data = []
        async for h in scan(aes):
            data.append(h)

    assert data == []
    assert aes.scroll.call_count == 0
    assert aes.clear_scroll.call_count == 1


async def test_logger(mocker):
    logger = mocker.patch('elasticsearch_adsl.scan.logger')

    aes = MagicMock(spec=AsyncElasticsearch)

    async def _failed_response():
        return {
            '_shards': {'total': 5, 'successful': 4},
            '_scroll_id': 42,
            'hits': {'hits': [1, 2, 3]},
        }
    aes.search.return_value = _failed_response()

    with raises(ScanError):
        async for _ in scan(aes, clear_scroll=False):
            pass
    assert logger.warning.call_count == 1


async def test_clear_scroll(aes, index_name, test_data, mocker):
    spy = mocker.patch.object(aes, 'clear_scroll', wraps=aes.clear_scroll)

    async for _ in scan(aes, index=index_name):
        pass
    assert spy.call_count == 1

    spy.reset_mock()
    try:
        async for _ in scan(aes, index=index_name, clear_scroll=False):
            pass
        assert spy.call_count == 0
    finally:
        await aes.transport.perform_request('DELETE', '/_search/scroll/_all')

