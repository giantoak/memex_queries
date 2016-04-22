es_url_format = 'https://{}:{}@{}/{}/'.format

CDR_URL = 'els.istresearch.com:19200/memex-domains'
CDR_INDEX = 'escorts'
CDR_AUTH_TUPLE = ('memex', 'qRJfu2uPkMLmH9cp')

cdr_url = es_url_format(CDR_AUTH_TUPLE[0],
                        CDR_AUTH_TUPLE[1],
                        CDR_URL,
                        CDR_INDEX)
local_es = None


def filter_terms_query(query_dict):
    """
    Takes a dict of parameters and returns a matching boolean filter
    :param dict query_dict:
    :return dict:
    """

    return {
        "filter": {
            "terms": query_dict
        }
    }


def _new_elasticsearch():
    """

    :return elasticsearch.Elasticsearch:
    """
    from elasticsearch import Elasticsearch
    global local_es
    if local_es is None:
        local_es = Elasticsearch(cdr_url)

    return local_es

def cdr_fields_for_cdr_ids(cdr_ids, fields=None, es=None):
    """
    Given a list of cdr_ids, returns a dict of ids with all of the requested fields.
    Per elasicsearch, if an entry lacks a field it is omitted.
    :param list|sr cdr_ids:
    :param list|str fields:
    :param elasticsearch.Elasticsearch es:
    :return dict:
    """

    if es is None:
        es = _new_elasticsearch()

    if fields is None:
        data_dict = es.search(body=filter_terms_query({'_id': cdr_ids}),
                              filter_path='hits.hits',
                              size=len(cdr_ids))
    else:
        data_dict = es.search(body=filter_terms_query({'_id': cdr_ids}),
                              filter_path='hits.hits',
                              fields=fields,
                              size=len(cdr_ids))

    out_dict = dict()
    for x in data_dict['hits']['hits']:
        out_dict[x] = dict()
        for y in x['fields']:
            if isinstance(x['fields'][y], list) and len(x['fields'][y]) == 1:
                out_dict[x][y] = x['fields'][y][0]
            else:
                out_dict[x][y] = x['fields'][y]

    return out_dict


def cdr_ad_ids_for_cdr_image_ids(cdr_image_ids, es=None):
    """

    :param list|str cdr_image_ids:
    :param elasticsearch.Elasticsearch es:
    :return list:
    """
    data_dict = cdr_fields_for_cdr_ids(cdr_image_ids, 'obj_parent', es)
    return [data_dict[x]['obj_parent'] for x in cdr_image_ids]


def cdr_image_ids_for_cdr_ad_ids(cdr_ad_ids, es=None):
    """
    Given a list of cdr_ad_ids or one cdr_ad_id, find the cdr_image_ids used with those ads.
    :param list|str cdr_ad_ids:
    :param elsasticsearch.Elasticsearch es:
    :return set:
    """
    if es is None:
        es = _new_elasticsearch()

    q = must_bool_filter_query({'obj_parent': cdr_ad_ids})
    data_dict = es.search(body=q, filter_path=['hits.hits._id'])
    return set(x['_id'] for x in data_dict['hits']['hits'])


def stored_url_of_cdr_image_id(cdr_image_id, es=None):
    """
    Given a cdr_image_id, return the URL where it has been stored
    :param str cdr_image_id:
    :param elasticsearch.Elasticsearch es:
    :return str:
    """
    data_dict = cdr_fields_for_cdr_ids(cdr_image_id, 'obj_stored_url', es)
    return data_dict[cdr_image_id]['obj_stored_url']
