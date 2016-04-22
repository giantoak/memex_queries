from .cdr import stored_url_of_cdr_image_id
from .cdr import cdr_ad_ids_for_cdr_image_ids
from .cdr import cdr_image_ids_for_cdr_ad_ids
from .hbase import cdr_id_from_dd_id
from .hbase import dd_id_from_cdr_id
from .hbase import dd_id_df
from .hbase import hbase_row_value
from .sqlite import dd_df_from_sqlite_tables


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
    return cdr_ad_ids_for_cdr_image_ids(cdr_image_id, es)


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

    return stored_url_of_cdr_image_id(cdr_image_id, es)


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
    return dd_id_df(cdr_ad_ids).join(dd_df_from_sqlite_tables([dd_id_from_cdr_id(x) for x in cdr_ad_ids],
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


def cdr_image_ids_for_dd_ad_id(dd_ad_id):
    """
    :param int dd_ad_id:
    :return list:
    """

    # The HBase table isn't yet fully populated, so we use sqlite
    # cdr_ad_id = hbase_row_value('deepdive_escort_ads', dd_ad_id, 'info:cdr_id')
    cdr_ad_id = dd_df_from_sqlite_tables([dd_ad_id], ['dd_id_to_cdr_id']).iloc[0, 1]
    ad_image_dict = cdr_image_ids_for_cdr_ad_ids(cdr_ad_id)
    return ad_image_dict[cdr_ad_id]
