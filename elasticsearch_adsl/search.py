from elasticsearch_dsl import search as sync_search, AttrDict

from elasticsearch_adsl.connections import get_connection
from elasticsearch_adsl.scan import scan


class Request(sync_search.Request):
    def __init__(self, using='async', *args, **kwargs):
        super(Request, self).__init__(using=using, *args, **kwargs)


class Search(Request, sync_search.Search):
    def __iter__(self):
        raise TypeError('Search is not iterable, use asynchronous iteration instead')

    async def __aiter__(self):
        for hit in await self.execute():
            yield hit

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

    async def delete(self):
        """delete() executes the query by delegating to delete_by_query()"""
        aes = get_connection(self._using)

        response = await aes.delete_by_query(
            index=self._index,
            body=self.to_dict(),
            doc_type=self._get_doc_type(),
            **self._params,
        )

        return AttrDict(response)

    async def scan(self):
        """
        Turn the search into a scan search and return a async generator that will
        iterate over all the documents matching the query.

        Use ``params`` method to specify any additional arguments you with to pass to the
        underlying ``scan`` helper
        """
        aes = get_connection(self._using)

        async for hit in scan(
                aes,
                query=self.to_dict(),
                index=self._index,
                doc_type=self._get_doc_type(),
                **self._params
        ):
            yield self._get_result(hit)
