from elasticsearch_adsl.scan import scan


async def test_scan_basic(aes, test_data, index_name):
    results = [r async for r in scan(aes, index=index_name)]
    assert len(results) == 3


async def test_preserve_order(aes, index_name, ):
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
    pass


async def test_initial_search_error(aes, index_name, ):
    pass


async def test_fast_error_route(aes, index_name, ):
    pass


async def test_logger(aes, index_name, ):
    pass


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

