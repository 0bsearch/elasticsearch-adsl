from elasticsearch_adsl.mapping import Mapping

_MAPPING = {
    'properties': {
        'created_at': {'type': 'date'}
    }
}


async def test_update_from_es(index_name, aes):
    await aes.indices.create(
        index=index_name,
        body={'mappings': _MAPPING}
    )

    m = Mapping()
    m.field('username', {'type': 'keyword'})
    await m.update_from_es(index=index_name)

    assert m.to_dict() == {
        'properties': {
            'created_at': {'type': 'date'},
            'username': {'type': 'keyword'},
        }
    }


async def test_from_es(index_name, aes):
    await aes.indices.create(
        index=index_name,
        body={'mappings': _MAPPING}
    )

    m = await Mapping.from_es(index_name)
    assert m.to_dict() == _MAPPING


async def test_save(index_name, aes):
    m1 = Mapping()
    m1.field('username', {'type': 'keyword'})
    await m1.save(index=index_name)

    m2 = await Mapping.from_es(index=index_name)
    assert m1.to_dict() == m2.to_dict()
