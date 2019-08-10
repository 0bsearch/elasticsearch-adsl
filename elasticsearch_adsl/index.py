# TODO: docs
from elasticsearch_dsl import index as sync_index

from .connections import get_connection
from .search import Search
# from .update_by_query import UpdateByQuery
# from .exceptions import IllegalOperation
from .mapping import Mapping
# from .utils import merge
# from . import analysis


class IndexTemplate(sync_index.IndexTemplate):
    async def save(self, using=None):
        aes = get_connection(using or self._index._using)
        return await aes.indices.put_template(name=self._template_name, body=self.to_dict())


class Index(sync_index.Index):
    # TODO: docstring; make attrs public in upstream
    def __init__(self, name, using='async'):
        super().__init__(name, using)

    # TODO: async mapping
    def mapping(self, mapping):
        """
        Associate a mapping (an instance of
        :class:`~elasticsearch_dsl.Mapping`) with this index.
        This means that, when this index is created, it will contain the
        mappings for the document type defined by those mappings.
        """
        self.get_or_create_mapping().update(mapping)

    # TODO: async mapping
    def get_or_create_mapping(self):
        if self._mapping is None:
            self._mapping = Mapping()
        return self._mapping

    # TODO: async mapping
    def load_mappings(self, using=None):
        self.get_or_create_mapping().update_from_es(self._name, using=using or self._using)

    def _get_connection(self, using=None):
        if self._name is None:
            raise ValueError('You cannot perform API calls on the default index.')
        return get_connection(using or self._using)
    connection = property(_get_connection)

    # TODO: async
    def document(self, document):
        """
        Associate a :class:`~elasticsearch_dsl.Document` subclass with an index.
        This means that, when this index is created, it will contain the
        mappings for the ``Document``. If the ``Document`` class doesn't have a
        default index yet (by defining ``class Index``), this instance will be
        used. Can be used as a decorator::

            i = Index('blog')

            @i.document
            class Post(Document):
                title = Text()

            # create the index, including Post mappings
            i.create()

            # .search() will now return a Search object that will return
            # properly deserialized Post instances
            s = i.search()
        """
        self._doc_types.append(document)

        # If the document index does not have any name, that means the user
        # did not set any index already to the document.
        # So set this index as document index
        if document._index._name is None:
            document._index = self

        return document

    # TODO: async
    def analyzer(self, *args, **kwargs):
        """
        Explicitly add an analyzer to an index. Note that all custom analyzers
        defined in mappings will also be created. This is useful for search analyzers.

        Example::

            from elasticsearch_dsl import analyzer, tokenizer

            my_analyzer = analyzer('my_analyzer',
                tokenizer=tokenizer('trigram', 'nGram', min_gram=3, max_gram=3),
                filter=['lowercase']
            )

            i = Index('blog')
            i.analyzer(my_analyzer)

        """
        analyzer = analysis.analyzer(*args, **kwargs)
        d = analyzer.get_analysis_definition()
        # empty custom analyzer, probably already defined out of our control
        if not d:
            return

        # merge the definition
        merge(self._analysis, d, True)

    def search(self, using=None):
        return Search(
            using=using or self._using,
            index=self._name,
            doc_type=self._doc_types
        )

    # TODO: async
    def updateByQuery(self, using=None):
        """
        Return a :class:`~elasticsearch_dsl.UpdateByQuery` object searching over the index
        (or all the indices belonging to this template) and updating Documents that match
        the search criteria.

        For more information, see here:
        https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
        """
        return UpdateByQuery(
            using=using or self._using,
            index=self._name,
        )

    # TODO: remove with elasticsearch-dsl >7.0.0
    def create(self, using=None, **kwargs):
        return self._get_connection(using).indices.create(
            index=self._name, body=self.to_dict(), **kwargs
        )

    async def is_closed(self, using=None):
        state = await self._get_connection(using).cluster.state(index=self._name, metric='metadata')
        return state['metadata']['indices'][self._name]['state'] == 'close'

    # TODO: async
    async def save(self, using=None):
        """
        Sync the index definition with elasticsearch, creating the index if it
        doesn't exist and updating its settings and mappings if it does.

        Note some settings and mapping changes cannot be done on an open
        index (or at all on an existing index) and for those this method will
        fail with the underlying exception.
        """
        if not await self.exists(using=using):
            return await self.create(using=using)

        body = self.to_dict()
        settings = body.pop('settings', {})
        analysis = settings.pop('analysis', None)
        current_settings = self.get_settings(using=using)[self._name]['settings']['index']
        if analysis:
            if self.is_closed(using=using):
                # closed index, update away
                settings['analysis'] = analysis
            else:
                # compare analysis definition, if all analysis objects are
                # already defined as requested, skip analysis update and
                # proceed, otherwise raise IllegalOperation
                existing_analysis = current_settings.get('analysis', {})
                if any(
                    existing_analysis.get(section, {}).get(k, None) != analysis[section][k]
                    for section in analysis
                    for k in analysis[section]
                ):
                    raise IllegalOperation(
                        'You cannot update analysis configuration on an open index, you need to close index %s first.' % self._name)

        # try and update the settings
        if settings:
            settings = settings.copy()
            for k, v in list(settings.items()):
                if k in current_settings and current_settings[k] == str(v):
                    del settings[k]

            if settings:
                self.put_settings(using=using, body=settings)

        # update the mappings, any conflict in the mappings will result in an
        # exception
        mappings = body.pop('mappings', {})
        if mappings:
            self.put_mapping(using=using, body=mappings)
