ES_URL = 'els.istresearch.com:19200/memex-domains'
ES_INDEX = 'escorts'
ES_AUTH_TUPLE = ('memex', 'qRJfu2uPkMLmH9cp')
es_url = 'https://{}:{}@{}/{}/'.format(ES_AUTH_TUPLE[0],
                                       ES_AUTH_TUPLE[1],
                                       ES_URL,
                                       ES_INDEX)
local_es = None


def must_bool_filter_query(query_dict):
    """
    Takes a dict of parameters and returns a matching boolean filter
    :param dict query_dict:
    :return dict:
    """
    return {
        "filter": {
            "bool": {
                "must": {
                    "term": query_dict
                }
            }
        }
    }


def new_elasticsearch():
    """

    :return elasticsearch.Elasticsearch:
    """
    from elasticsearch import Elasticsearch
    global local_es
    if local_es is None:
        local_es = Elasticsearch(es_url)

    return local_es