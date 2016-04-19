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


def cdr_ad_ids_for_cdr_image_ids(cdr_image_ids, es=None):
    """

    :param list|str cdr_image_ids:
    :param elasticsearch.Elasticsearhc es:
    :return list:
    """
    if es is None:
        es = new_elasticsearch()

    data_dict = es.search(body=must_bool_filter_query({'_id': cdr_image_ids}),
                          filter_path=['hits.hits'],
                          fields=['obj_parent'])
    return [x['fields']['object_parent'] for x in data_dict['hits']['hits']]


def cdr_image_ids_for_cdr_ad_ids(cdr_ad_ids, es=None):
    """

    :param list|str cdr_ad_ids:
    :param elsasticsearch.Elasticsearch es:
    :return list:
    """

    if es is None:
        es = new_elasticsearch()

    q = must_bool_filter_query({'obj_parent': cdr_ad_ids})
    data_dict = es.search(body=q, filter_path=['hits.hits._id'])
    return [x['_id'] for x in data_dict['hits']['hits']]


def stored_url_of_cdr_image_id(cdr_image_id, es=None):
    """

    :param cdr_image_id:
    :param es:
    :return:
    """
    if es is None:
        es = new_elasticsearch()

    q = must_bool_filter_query({'_id': cdr_image_id})

    try:
        data_dict = es.search(body=q, filter_path=['hits.hits._source'])
        return data_dict['hits']['hits'][0]['_source']['obj_stored_url']
    except:
        return None
