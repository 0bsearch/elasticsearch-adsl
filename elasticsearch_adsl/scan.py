import logging

from elasticsearch.helpers import ScanError


logger = logging.getLogger(__name__)


async def scan(
        client,
        query=None,
        scroll='5m',
        raise_on_error=True,
        preserve_order=False,
        size=1000,
        request_timeout=None,
        clear_scroll=True,
        scroll_kwargs=None,
        **kwargs
):
    """
    Simple helper on top of search+scroll machinery, for easier data retrieval,
    acts as async generator.

    Args:
        client (AsyncElasticsearch): elasticsearch client to use
        query (dict): query for initial search query
        scroll (str): specify how long ES should keep search context alive. Default is '5m'
        raise_on_error (bool): if to raise `ScanError` when some ES shards fail.
            Defaults to `True`
        preserve_order (bool): if to preserve query ordering. ES has optimization for faster
            scrolling, when sorting by `_doc`. Defaults to `False`, to give better performance
        size (int): document batch size for every request. Higher value leads to heavier
            memory usage, but less network round-trips. Defaults to `1000`
        request_timeout (Real): request timeout, in seconds
        clear_scroll (bool): if to clear scroll context in ES. Defaults to `True`
        scroll_kwargs (dict): extra keyword parameters to pass for every `scroll` query
        **kwargs: all additional arguments to pass into initial `search` call
    """
    scroll_kwargs = scroll_kwargs or {}

    if not preserve_order:
        query = query.copy() if query else {}
        query["sort"] = "_doc"

    resp = await client.search(
        body=query,
        scroll=scroll,
        size=size,
        request_timeout=request_timeout,
        **kwargs
    )
    scroll_id = resp.get('_scroll_id')

    try:
        while scroll_id and resp['hits']['hits']:
            successful = resp['_shards']['successful']
            total = resp['_shards']['total']
            if successful < total:
                msg_template = 'Scroll request has only succeeded on %d shards out of %d.'
                logger.warning(msg_template, successful, total)
                if raise_on_error:
                    raise ScanError(scroll_id, msg_template % (successful, total))

            for hit in resp['hits']['hits']:
                yield hit

            resp = await client.scroll(
                scroll_id,
                scroll=scroll,
                request_timeout=request_timeout,
                **scroll_kwargs
            )
            scroll_id = resp.get('_scroll_id')

    finally:
        if scroll_id and clear_scroll:
            await client.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404, ))
