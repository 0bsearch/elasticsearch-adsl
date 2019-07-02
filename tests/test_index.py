from inspect import getmembers, ismethod

from elasticsearch_adsl.connections import create_connection
from elasticsearch_adsl.index import Index


# TODO: move test to dsl upstream & implement missing methods
async def test_proxy_methods(aes):
    not_implemented = {
        'delete_template',
        'exists_template',
        'get_template',
        'put_template',
        'rollover',
        'split',
        'update_aliases',
    }

    for name, _ in getmembers(aes.indices, predicate=ismethod):
        if name in not_implemented:
            continue
        assert hasattr(Index, name)


async def test_is_closed(aes, index_name):
    index = Index(name=index_name)
    await index.create()

    assert await index.is_closed() is False
    await index.close()
    assert await index.is_closed() is True
    await index.open()
    assert await index.is_closed() is False


async def test_search(aes, test_data, index_name):
    index = Index(index_name)

    search = index.search()
    assert search._using == index._using
    assert search._index == [index._name]

    results = await search.execute()
    assert len(results) == 3


async def test_init(index_name):
    index = Index(name=index_name)
    assert index._using == 'async'

    index = Index(name=index_name, using='other')
    assert index._using == 'other'


async def test_connection(index_name, clean_connections):
    async_conn = create_connection()
    other_conn = create_connection('other')

    index = Index(index_name)
    assert index._using == 'async'
    assert index.connection is async_conn
    assert index._get_connection() is async_conn
    assert index._get_connection('other') is other_conn

    index = Index(index_name, using='other')
    assert index._using == 'other'
    assert index.connection is other_conn
    assert index._get_connection() is other_conn
    assert index._get_connection('async') is async_conn
