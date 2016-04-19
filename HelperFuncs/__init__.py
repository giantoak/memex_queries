from .elasticsearch_helpers import new_elasticsearch
from .elasticsearch_helpers import must_bool_filter_query
from .hbase_helpers import dd_id
from .hbase_helpers import dd_id_df
from .hbase_helpers import hbase_row_value
from .sqlite_helpers import dd_df_from_sqlite_tables


def cdr_ad_ids_for_general_cdr_image_id(cdr_image_id, es=None):
    """
    Given the ID of an image in the CDR, get the IDs of the ads in the CDR in which it was used
    :param str cdr_image_id: The CDR ID of an image
    :param elasticsearch.Elasticsearch es:
    :return list: A list of the CDR Ad IDs in which the image was used.
    """
    cur_hash = image_hash(cdr_image_id, es)

    # TODO: Hit Gabriel's table of hashes instead of Svebor's table or ES.

    # Check Svebor's table for a copy of the hashed image
    # and the ads in which it was use
    ad_ids = hbase_row_value('ht_images_infos_2016',
                             cur_hash,
                             'info:all_parent_ids')
    if ad_ids is not None:
        return ad_ids.split(',')

    # Our fallback is to hit elastic and get every parent of the current image
    if es is None:
        es = new_elasticsearch()

    data_dict = es.search(body=must_bool_filter_query({'_id': cdr_image_id}),
                          filter_path=['hits.hits'],
                          fields=['obj_parent'])
    return [x['fields']['object_parent'] for x in data_dict['hits']['hits']]


def image_stored_url(cdr_image_id, es=None):
    """
    Checks various MEMEX sources for the location at which a particular
    image's raw binary is stored.
    :param str cdr_image_id: A CDR Image ID
    :param elasticsearch.Elasticsearch es:
    :return str: The image's stored URL, or None
    """
    try:
        image_json = hbase_row_value('dig_isi_cdr2_ht_images',
                                         cdr_image_id,
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
        q = must_bool_filter_query({'_id': cdr_image_id})
        data_dict = es.search(body=q, filter_path=['hits.hits._source'])
        image_url = data_dict['hits']['hits'][0]['_source']['obj_stored_url']
        return image_url
    except:
        pass

    return None


def image_hash(cdr_image_id, es=None):
    """
    Checks various MEMEX resources for the image hash.
    If we can't find it, calculate the hash.
    :param str cdr_image_id: A CDR Image ID
    :param elasticsearch.Elasticsearch es:
    :return str: The image's hash
    """
    import requests

    # Check Svebor's hash lookup table
    image_hash = hbase_row_value('ht_images_cdrid_to_sha1_2016',
                                 cdr_image_id,
                                 'hash:sha1')
    if image_hash is not None:
        return image_hash

    # TODO Check if the CDR has a saved copy of the hash

    # Get raw image data, then calculate the hash.
    image_url = image_stored_url(cdr_image_id, es)
    r = requests.get(image_url)
    data = r.text

    from hashlib import sha1
    h = sha1()
    h.update(data.encode('utf8'))
    return h.hexdigest().upper()


def post_dates_for_cdr_ad_ids(cdr_ad_ids):
    """
    Given a list of CDR Ad IDs, find their DD IDs and when they were posted
    :param list|set cdr_ad_ids: An iterable of CDR Ad IDs
    :return pandas.DataFrame: DataFrame of CDR IDs, DD IDs, and Post Dates
    """
    return dd_id_df(cdr_ad_ids).join(dd_df_from_sqlite_tables([dd_id(x) for x in cdr_ad_ids],
                                                              'dd_id_to_post_date'),
                                     on=['dd_id'])


def post_dates_for_general_cdr_image_id(cdr_image_id, es=None):
    """
    Given the ID of an image in the CDR, get all of the timestamps on which it was used.
    :param str cdr_image_id: A CDR Image ID
    :param elasticsearch.Elasticsearch es:
    :return pandas.DataFrame: DataFrame of CDR IDs, DD IDs, and Post Dates
    """
    return post_dates_for_cdr_ad_ids(cdr_ad_ids_for_general_cdr_image_id(cdr_image_id, es))

