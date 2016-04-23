CDR_URL = 'els.istresearch.com:19200/memex-domains'
CDR_INDEX = 'escorts'
CDR_AUTH_TUPLE = ('memex', 'qRJfu2uPkMLmH9cp')

cdr_url = 'https://{}:{}@{}/{}/'.format(CDR_AUTH_TUPLE[0],
                                        CDR_AUTH_TUPLE[1],
                                        CDR_URL,
                                        CDR_INDEX)
local_es = None


def _filter_terms_query(query_dict):
    """
    :param dict query_dict: Dict of CDR terms/fields to filter on and their assigned values
    :returns: `dict` -- The query_dict, wrapped in Elastic "filter: terms" JSON
    """
    return {
        "filter": {
            "terms": query_dict
        }
    }


def _new_elasticsearch():
    """
    :returns: `elasticsearch.Elasticsearch` -- An active connection to the CDR
    """
    from elasticsearch import Elasticsearch
    global local_es
    if local_es is None:
        local_es = Elasticsearch(cdr_url)

    return local_es


def cdr_fields_for_cdr_ids(cdr_ids, fields=None, es=None):
    """
    :param list|str cdr_ids: Single CDR ID or a list of CDR IDs
    :param list|str fields: List of desired fields. If None, returns all \
    fields
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `dict(dict)` -- Dict keyed on CDR IDs. Values are dictionaries of \
    field values keyed on field names. Null fields are omitted. Unlike CDR \
    JSON, lists with one value are collapsed to that value.
    """
    if es is None:
        es = _new_elasticsearch()

    if fields is None:
        data_dict = es.search(body=_filter_terms_query({'_id': cdr_ids}),
                              filter_path='hits.hits',
                              size=len(cdr_ids))
    else:
        data_dict = es.search(body=_filter_terms_query({'_id': cdr_ids}),
                              filter_path='hits.hits',
                              fields=fields,
                              size=len(cdr_ids))

    out_dict = dict()
    for hit in data_dict['hits']['hits']:
        out_dict[hit['_id']] = dict()
        if 'fields' not in hit:
            continue
        for x in hit['fields']:
            if isinstance(hit['fields'][x], list) and len(hit['fields'][x]) == 1:
                out_dict[hit['_id']][x] = hit['fields'][x][0]
            else:
                out_dict[hit['_id']][x] = hit['fields'][x]

    return out_dict


def cdr_ad_ids_for_cdr_image_ids(cdr_image_ids, es=None):
    """
    :param list|str cdr_image_ids: Single CDR ID of an image or a list of \
    CDR IDs of images.
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `list` -- List of CDR IDs of ads using these images
    """
    data_dict = cdr_fields_for_cdr_ids(cdr_image_ids, 'obj_parent', es)
    return [data_dict[x]['obj_parent'] for x in cdr_image_ids]


def cdr_image_ids_for_cdr_ad_ids(cdr_ad_ids, es=None):
    """
    :param list|str cdr_ad_ids: Single CDR ID of an ad or a list of CDR IDs \
    of ads.
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `dict` -- Dict of list of CDR IDS of images, keyed on the CDR IDs of \
    ads using those images.
    """
    if es is None:
        es = _new_elasticsearch()

    res_dict = {cdr_ad_id: [] for cdr_ad_id in cdr_ad_ids}

    data_dict = es.search(body=_filter_terms_query({'obj_parent': cdr_ad_ids}),
                          filter_path=['hits.hits'],
                          fields='obj_parent')
    if len(data_dict) < 1:
        return res_dict

    for hit in data_dict['hits']['hits']:
        res_dict[hit['fields']['obj_parent'][0]].append(hit['_id'])

    return res_dict


def stored_url_of_cdr_image_id(cdr_image_id, es=None):
    """
    :param str cdr_image_id: CDR ID of an image
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `str` -- URL where the image is stored
    """
    data_dict = cdr_fields_for_cdr_ids(cdr_image_id, 'obj_stored_url', es)
    return data_dict[cdr_image_id]['obj_stored_url']
