ES_URL = 'els.istresearch.com:19200/memex-domains'
ES_INDEX = 'escorts'
ES_AUTH_TUPLE = ('memex', 'qRJfu2uPkMLmH9cp')
HBASE_URL = 'memex-hbase-master:8080'
local_es = None

es_url = 'https://{}:{}@{}/{}/'.format(ES_AUTH_TUPLE[0],
                                       ES_AUTH_TUPLE[1],
                                       ES_URL,
                                       ES_INDEX)


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


def hbase_row_value(table, row_id, key_id):
    """
    :param str table: The name of the MEMEX HBase table
    :param str row_id: The row to get from the table
    :param str key_id: The key to get from the row
    :return str: the value in the desired key, or None
    """
    import requests
    try:
        hbase_url = 'http://{}/{}/{}/{}'.format(HBASE_URL, table, row_id, key_id)
        r = requests.get(hbase_url)
        if r.status_code == 200:
            return r.text
    except:
        pass

    return None


def dd_id(cdr_id):
    """
    Query HBase to to get the Lattice / Deep Dive Dump ID that maps to the corresponding CDR ID
    :param str cdr_id:
    :return str:
    """
    return hbase_row_value('cdr_id_to_dd_id', cdr_id, 'info:dd_id')


def dd_id_df(cdr_ids):
    from pandas import DataFrame
    return DataFrame({'cdr_id': cdr_ids,
                      'dd_id': [dd_id(cdr_id) for cdr_id in cdr_ids]})


def image_stored_url(image_id, es=None):
    """
    Checks various MEMEX sources for the location at which a particular
    image's raw binary is stored.
    :param str image_id:
    :param elasticsearch.Elasticsearch es:
    :return str:
    """
    try:
        image_json = hbase_row_value('dig_isi_cdr2_ht_images',
                                         image_id,
                                         'images:images')
        image_url = eval(image_json)['obj_stored_url']
        if image_url is not None:
            return image_url
    except:
        pass

    try:
        # Get the url from elastic
        if es is None:
             es = new_elasticsearch()
        q = must_bool_filter_query({'_id': image_id})
        data_dict = es.search(body=q, filter_path=['hits.hits._source'])
        image_url = data_dict['hits']['hits'][0]['_source']['obj_stored_url']
        return image_url
    except:
        pass

    return None


def image_hash(image_id, es=None):
    """
    Checks various MEMEX resources for the image hash.
    If we can't find it, calculate the hash.
    :param str image_id:
    :param elasticsearch.Elasticsearch es:
    :return str:
    """
    import requests

    # Check Svebor's hash lookup table
    image_hash = hbase_row_value('ht_images_cdrid_to_sha1_2016',
                                     image_id,
                                     'hash:sha1')
    if image_hash is not None:
        return image_hash

    # Get raw image data, then calculate the hash.
    image_url = image_stored_url(image_id, es)
    r = requests.get(image_url)
    data = r.text

    from hashlib import sha1
    h = sha1()
    h.update(data.encode('utf8'))
    return h.hexdigest().upper()


def timestamp_for_cdr_id(cdr_id, es=None):
    """
    Return the timestamp for this CDR ID
    :param str cdr_id: An entity's CDR ID
    :param elasticsearch.Elasticsearch es:
    :return int:
    """
    data_str = hbase_row_value('dig_isi_cdr2_ht_images',
                                   cdr_id,
                                   'images:images')
    if data_str is not None:
        return eval(data_str)['timestamp']

    data_str = hbase_row_value('ht_images_cdrid_to_image_ht_id',
                                   cdr_id,
                                   'info:timestamp')
    if data_str is not None:
        return int(data_str)

    q = must_bool_filter_query({'_id': cdr_id})
    if es is None:
        es = new_elasticsearch()
    data_dict = es.search(body=q, filter_path=['hits.hits._source'])
    if data_dict is not None:
        return data_dict['hits']['hits'][0]['_source']['timestamp']
    
    return None


def all_ad_ids_for_cdr_image_id(image_id, es=None):
    """
    Given the ID of an image in the CDR, get all of the ads in which it was used.
    :param str image_id: The CDR ID of an image
    :param elasticsearch.Elasticsearch es:
    :return list: A list of timestamps since the epoch on which the timestamp was used.
    """
    cur_hash = image_hash(image_id, es)

    # Check Svebor's table for a copy of the hashed image, and get the other
    # images.
    ad_ids = hbase_row_value('ht_images_infos_2016',
                             cur_hash,
                             'info:all_parent_ids')
    if ad_ids is not None:
        return ad_ids.split(',')

    # Our fallback is to hit elastic and get every parent of the current image
    if es is None:
        es = new_elasticsearch()

    data_dict = es.search(body=must_bool_filter_query({'_id': image_id}),
                          filter_path=['hits.hits'],
                          fields=['obj_parent'])
    return [x['fields']['object_parent'] for x in data_dict['hits']['hits']]


def all_timestamps_for_cdr_image(image_id, es=None):
    """
    Given the ID of an image in the CDR, get all of the timestamps on which it was used.
    :param str image_id: The CDR ID of an image
    :param elasticsearch.Elasticsearch es:
    :return list: A list of timestamps since the epoch on which the timestamp was used.
    """

    ad_ids = all_ad_ids_for_cdr_image_id(image_id, es=None)

    # Ad data _only_ lives in the CDR.
    # This should, however, get swapped for the lattice dumps, which have the extractions we need.
    if es is None:
        es = new_elasticsearch()

    data_dict = es.search(body=must_bool_filter_query({'_id': ad_ids}),
                          filter_path=['hits.hits'],
                          fields=['timestamp'],
                          size=len(ad_ids))
    return [x['fields']['timestamp'] for x in data_dict['hits']['hits']]
