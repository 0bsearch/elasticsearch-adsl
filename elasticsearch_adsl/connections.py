from elasticsearch_async import AsyncElasticsearch
from elasticsearch_dsl import connections as sync
from elasticsearch_dsl.serializer import serializer


class _Connections(sync.Connections):
    def __init__(self):
        super().__init__()
        self._kwargs = sync.connections._kwargs
        self._conns = sync.connections._conns

    def create_connection(self, alias='async', client_class=AsyncElasticsearch, **kwargs):
        """
        Construct an instance of ``client_class`` and register
        it under given alias.
        """
        kwargs.setdefault('serializer', serializer)
        conn = self._conns[alias] = client_class(**kwargs)
        return conn

    def get_connection(self, alias='async'):
        return super().get_connection(alias)

    get_connection.__doc__ = sync.Connections.get_connection.__doc__


_connections = _Connections()
configure = _connections.configure
add_connection = _connections.add_connection
remove_connection = _connections.remove_connection
create_connection = _connections.create_connection
get_connection = _connections.get_connection
