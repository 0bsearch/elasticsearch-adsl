from elasticsearch_async import AsyncElasticsearch
from elasticsearch_dsl import connections
from elasticsearch_dsl.serializer import serializer


def create_connection(alias='async', client_class=AsyncElasticsearch, **kwargs):
    """
    Construct an instance of ``client_class`` and register
    it under given alias.
    """
    kwargs.setdefault('serializer', serializer)
    conn = connections.connections._conns[alias] = client_class(**kwargs)
    return conn


def get_connection(alias='async'):
    return connections.get_connection(alias)


get_connection.__doc__ = connections.get_connection.__doc__


configure = connections.configure
add_connection = connections.add_connection
remove_connection = connections.remove_connection
