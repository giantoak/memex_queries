from .cdr import stored_url_of_cdr_image_id
from .cdr import cdr_ad_ids_for_cdr_image_ids
from .cdr import cdr_fields_for_cdr_ids
from .cdr import cdr_image_ids_for_cdr_ad_ids
from .hbase import cdr_id_from_dd_id
from .hbase import dd_id_from_cdr_id
from .hbase import dd_id_df
from .hbase import hbase_row_value
from .sqlite import dd_df_from_sqlite_tables


def cdr_ad_ids_for_general_cdr_image_id(cdr_image_id, es=None):
    """
    :param str cdr_image_id: The CDR ID of an image
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `list` -- A list of the CDR Ad IDs in which the image was used.
    """
    cur_hash = image_hash(cdr_image_id, es)

    # TODO: Hit Gabriel's table of hashes instead of Svebor's table or ES.

    # Check Svebor's table for a copy of the hashed image
    # and the ads in which it was used
    ad_ids = hbase_row_value('ht_images_infos_2016',
                             cur_hash,
                             'info:all_parent_ids')
    if ad_ids is not None:
        return ad_ids.split(',')

    # Our fallback is to hit elastic and get every parent of the *speicific& current image
    return cdr_ad_ids_for_cdr_image_ids(cdr_image_id, es)


def image_stored_url(cdr_image_id, es=None):
    """
    :param str cdr_image_id: A CDR Image ID
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `str` -- The image's stored URL, or `None`
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


def image_hashes(cdr_image_ids, es=None):
    """
    * Check various MEMEX resources for the SHA1 hashes of `cdr_image_ids`
    * If they can't be found, calculate the hashes
    * Returns an ordered list of the results.

    :param list cdr_image_ids: A list of CDR Image IDs
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `list(str)` -- A list of hashes of `cdr_image_ids`
    """
    import requests

    # this is a stupid workaround; should use a list
    hash_dict = dict()

    # Check Svebor's hash lookup table
    # Return if we find everything
    for cdr_image_id in cdr_image_ids:
        hash_dict[cdr_image_id] = hbase_row_value('ht_images_cdrid_to_sha1_2016',
                                                  cdr_image_id,
                                                  'hash:sha1')

    missing_image_cdr_ids = [x for x in hash_dict if hash_dict[x] is None]
    if len(missing_image_cdr_ids) == 0:
        return [hash_dict[cdr_image_id] for cdr_image_id in cdr_image_ids]

    # Hit the CDR for original URLs and lookup ids for image_hash
    data_dict = cdr_fields_for_cdr_ids(missing_image_cdr_ids, ['crawl_data.image_id', 'obj_stored_url'], es)
    for cdr_image_id in data_dict:
        if 'crawl_data.image_id' in data_dict[cdr_image_id]:
            hash_dict[cdr_image_id] = hbase_row_value('image_hash',
                                                      data_dict[cdr_image_id]['crawl_data.image_id'],
                                                      'image:hash')

        if hash_dict[cdr_image_id] is None:
            if 'obj_stored_url' not in data_dict:
                # No stored URL, so nothing to be done
                continue

            r = requests.get(data_dict['obj_stored_url'])
            data = r.text

            from hashlib import sha1
            h = sha1()
            h.update(data.encode('utf8'))
            hash_dict[cdr_image_id] = h.hexdigest().upper()

    return [hash_dict[cdr_image_id] for cdr_image_id in cdr_image_ids]


def image_hash(cdr_image_id, es=None):
    """
    Invokes `image_hashes` on a single `cdr_image_id`

    :param str cdr_image_id: A CDR Image ID
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `str` -- SHA1 hash of `cdr_image_id`
    """
    return image_hashes([cdr_image_id], es)[0]


def post_dates_for_cdr_ad_ids(cdr_ad_ids):
    """
    Given a list of CDR Ad IDs, find their DD IDs and when they were posted.

    :param list cdr_ad_ids: A list of CDR IDs of escort ads.
    :returns: `pandas.DataFrame` -- DataFrame of CDR IDs, DD IDs, and Post Dates
    """
    return dd_id_df(cdr_ad_ids).join(dd_df_from_sqlite_tables([dd_id_from_cdr_id(x) for x in cdr_ad_ids],
                                                              'dd_id_to_post_date'),
                                     on=['dd_id'])


def post_dates_for_general_cdr_image_id(cdr_image_id, es=None):
    """
    Given the ID of an image in the CDR, get all of the dates on which it \
    was posted.

    :param str cdr_image_id: An image's CDR ID
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `pandas.DataFrame` -- DataFrame of CDR IDs, Deep Dive IDs, and \
    Post Dates
    """
    return post_dates_for_cdr_ad_ids(cdr_ad_ids_for_general_cdr_image_id(cdr_image_id, es))


def cdr_image_ids_for_dd_ad_id(dd_ad_id):
    """
    :param int dd_ad_id: The Deep Dive ID of an escort ad.
    :returns: `list` -- The list of CDR IDs of the images used with this ad.
    """

    # The HBase table isn't yet fully populated, so we use sqlite
    # cdr_ad_id = hbase_row_value('deepdive_escort_ads', dd_ad_id, 'info:cdr_id')
    cdr_ad_id = dd_df_from_sqlite_tables([dd_ad_id], ['dd_id_to_cdr_id']).iloc[0, 1]
    ad_image_dict = cdr_image_ids_for_cdr_ad_ids(cdr_ad_id)
    return ad_image_dict[cdr_ad_id]
