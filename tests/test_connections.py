from elasticsearch import JSONSerializer
from elasticsearch_async import AsyncElasticsearch
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.serializer import AttrJSONSerializer
from pytest import raises, fixture

from elasticsearch_adsl.connections import create_connection, get_connection


@fixture
def clean_connections():
    connections._kwargs.clear()
    connections._conns.clear()


async def test_create_connection_base(clean_connections):
    conn = create_connection()
    assert isinstance(conn, AsyncElasticsearch)
    assert conn is get_connection()


async def test_create_connection_serializer(clean_connections):
    conn = create_connection()
    assert isinstance(conn.transport.serializer, AttrJSONSerializer)

    conn = create_connection(serializer=JSONSerializer())
    assert isinstance(conn.transport.serializer, JSONSerializer)


async def test_get_connection(clean_connections):
    conn = create_connection()
    assert conn is get_connection()
    assert conn is get_connection('async')
    assert  conn is connections.get_connection('async')

    with raises(KeyError):
        get_connection('default')
        connections.get_connection()

