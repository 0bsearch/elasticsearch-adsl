from elasticsearch_dsl import search as sync_search

from elasticsearch_adsl.connections import get_connection


class Request(sync_search.Request):
    def __init__(self, using='async', *args, **kwargs):
        super(Request, self).__init__(using=using, *args, **kwargs)


class Search(Request, sync_search.Search):
    def __iter__(self):  # TODO: raise exception, implement aiter
        pass

    async def count(self):
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        if hasattr(self, '_response'):
            return self._response.hits.total

        aes = get_connection(self._using)
        d = self.to_dict(count=True)
        response = await aes.count(
            index=self._index,
            doc_type=self._get_doc_type(),
            body=d,
            **self._params
        )
        return response['count']

    async def execute(self, ignore_cache=False):
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        if ignore_cache or not hasattr(self, '_response'):
            aes = get_connection(self._using)

            response = await aes.search(
                index=self._index,
                doc_type=self._get_doc_type(),
                body=self.to_dict(),
                **self._params,
            )
            self._response = self._response_class(self, response)

        return self._response
