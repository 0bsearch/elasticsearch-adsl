from elasticsearch_dsl import mapping as sync

from .connections import get_connection


class Mapping(sync.Mapping):
    @classmethod
    async def from_es(cls, index, using='async'):
        m = cls()
        await m.update_from_es(index, using)
        return m

    def save(self, index, using='async'):
        from .index import Index
        index = Index(index, using=using)
        index.mapping(self)
        return index.save()

    async def update_from_es(self, index, using='async'):
        aes = get_connection(using)
        raw = await aes.indices.get_mapping(index=index)
        _, raw = raw.popitem()
        self._update_from_dict(raw['mappings'])
