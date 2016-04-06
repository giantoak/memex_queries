ES_URL = 'els.istresearch.com:19200/memex-domains'
ES_INDEX = 'escorts'
ES_AUTH_TUPLE = ('memex', 'qRJfu2uPkMLmH9cp')
HBASE_URL = 'memex-hbase-master:8080'
local_es = None

es_url = 'https://{}:{}@{}/{}/'.format(ES_AUTH_TUPLE[0],
                                       ES_AUTH_TUPLE[1],
                                       ES_URL,
                                       ES_INDEX)


def get_must_bool_filter_query(query_dict):
    """
    Takes a dict of paramters and returns a matching boolean filter
    :param dict query_dict:
    :returns dict:
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


def get_new_elasticsearch():
    from elasticsearch import Elasticsearch
    if local_es is None:
        local_es = Elasticsearch(es_url)

    return local_es


def get_hbase_row_value(table, row_id, key_str):
    """
    :param str table: The name of the MEMEX HBase table
    :param str row_id: The row to get from the table
    :param str key_id: The key to get from the row
    :returns str: the value in the desired key, or None
    """
    import requests
    try:
        hbase_url = 'http://{}/{}/{}/{}'.format(HBASE_URL, row_id, key_str)
        r = requests.get(hbase_url)
        if r.status_code == 200:
            return r.text
    except:
        pass
    return None


def get_first_cdr_entry_matching_dict(term_dict, es=None):
    """
    Get the first result matching a set of hard identifiers back frm the CDR.
    :param dict term_dict:
    :param elasticsearch.Elasticsearch es:
    :param tuple auth_tuple:
    :return dict:
    """
    if es is None:
        es = get_new_elasticsearch(auth_tuple)
    q = {
        "filter": {
            "bool": {
                "must": {
                    "term": term_dict
                }
            }
        }
    }
    es.search(body=q)


def get_image_stored_url(image_id, es=None):
    """
    Checks various MEMEX sources for the location at which a particular
    image's raw binary is stored.
    :param str image_id:
    :param elasticsearch.Elasticsearch es:
    :param tuple auth_tuple:
    :return str:
    """
    try:
        image_json = get_hbase_row_value('dig_isi_cdr2_ht_images',
                                         image_id,
                                         'images:images')
        image_url = eval(image_json)['obj_stored_url']
        if image_url is not None:
            return image_url
    except:
        pass
    try:
        # Get the url from elastic
        # if es is None:
        #     es = get_new_elasticsearch(auth_tuple)
        q = get_must_bool_filter_query({'_id': image_id})
        # r = es.get(index=ES_INDEX, doc_type='_all', id=image_id,
        # fields=['obj_stored_url'])
        import requests
        es_url = es_url_fmt(auth_tuple)
        r = requests.get(es_url, data=json.dumps(q))
        image_url = r.json()['hits']['hits'][0]['_source']['obj_stored_url']
        return image_url
    except:
        pass

    return None


def get_image_hash(image_id, es=None, auth_tuple=None):
    """
    Checks various MEMEX resources for the image hash.
    If we can't find it, calculate the hash.
    :param str image_id:
    :returns str:
    """
    import requests
    # Check Svebor's hash lookup table
    image_hash = get_hbase_row_value('ht_images_cdrid_to_sha1_2016',
                                     image_id,
                                     'hash:sha1')
    if image_hash is not None:
        return image_hash

    # Get raw image data, then calculate the hash.
    image_url = get_image_stored_url(image_id, es, auth_tuple)
    r = requests.get(image_url)
    data = r.text

    from hashlib import sha1
    h = sha1()
    h.update(data.encode('utf8'))
    return h.hexdigest().upper()

def get_timestamp_for_id(cdr_id):
    """

    :param str cdr_id:
    """
    data_str = get_hbase_row_value('dig_isi_cdr2_ht_images',
                                   cdr_id,
                                   'images:images')
    if data_str is not None:
        return eval(data_str)['timestamp']

    data_str = get_hbase_row_value('ht_images_cdrid_to_image_ht_id',
                                   cdr_id,
                                   'info:timestamp')
    if data_str is not None:
        return int(data_str)

    q = get_must_bool_filter_query({'_id': cdr_id})
    if es is None:
         es = get_new_elasticsearch(auth_tuple)
    data_dict = es.search(body=q, filter_path=['hits.hits._source'])
    if data_dict is not None:
        return data_dict['hits']['hits'][0]['_source']['timestamp']
    
    return None
