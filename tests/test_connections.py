from elasticsearch import JSONSerializer
from elasticsearch_async import AsyncElasticsearch
from elasticsearch_dsl import connections
from elasticsearch_dsl.serializer import AttrJSONSerializer
from pytest import raises, fixture

from elasticsearch_adsl.connections import (
    add_connection,
    configure,
    create_connection,
    get_connection,
    remove_connection,
    sync
)


@fixture(autouse=True)
async def _clean_connections(clean_connections):
    return


async def test_create_connection_base():
    conn = create_connection()
    assert isinstance(conn, AsyncElasticsearch)
    assert conn is get_connection()


async def test_create_connection_serializer():
    conn = create_connection()
    assert isinstance(conn.transport.serializer, AttrJSONSerializer)

    conn = create_connection(serializer=JSONSerializer())
    assert isinstance(conn.transport.serializer, JSONSerializer)


async def test_get_connection():
    conn = create_connection()
    assert conn is get_connection()
    assert conn is get_connection('async')
    assert conn is connections.get_connection('async')

    with raises(KeyError):
        get_connection('default')
        connections.get_connection()


async def test_get_with_configure():
    configure(dev={'host': 'localhost'})
    conn = get_connection('dev')
    assert isinstance(conn, AsyncElasticsearch)


def test_docstrings():
    assert create_connection.__doc__
    assert get_connection.__doc__
    assert add_connection.__doc__
    assert remove_connection.__doc__


def test_sync_proxy():
    assert sync.create_connection is connections.create_connection
    assert sync.configure is connections.configure
    assert sync.add_connection is connections.add_connection
    assert sync.remove_connection is connections.remove_connection
    assert sync.create_connection is connections.create_connection
    assert sync.get_connection is connections.get_connection
