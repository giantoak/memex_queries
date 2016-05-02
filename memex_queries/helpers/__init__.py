from .isi.hbase import cdr_ad_ids_for_image_hash
from .isi.hbase import image_hash_for_cdr_image_id
from .ist.cdr import cdr_ad_ids_for_cdr_image_ids
from .ist.cdr import cdr_fields_for_cdr_ids
from .ist.cdr import cdr_image_ids_for_cdr_ad_ids
from .lattice.sqlite import df_of_tables_for_cdr_ad_ids
from .uncharted.hbase import image_hash_for_memex_ht_id
import pandas as pd


def cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id, es=None):
    """
    :param str cdr_image_id: The CDR ID of an image
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `list` -- A list of the CDR Ad IDs in which the image was used.
    """
    cur_hash = image_hash(cdr_image_id, es)

    # TODO: Hit Gabriel's table of hashes instead of Svebor's table or ES.

    # Check Svebor's table for a copy of the hashed image
    # and the ads in which it was used
    ad_ids = cdr_ad_ids_for_image_hash(cur_hash)
    if ad_ids is not None:
        return ad_ids

    # Our fallback is to hit elastic and get every parent of
    # the *specific* current image
    return cdr_ad_ids_for_cdr_image_ids([cdr_image_id], es)[0]


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

    # Populate hash dict with ISI table data
    hash_dict = {cdr_image_id: image_hash_for_cdr_image_id(cdr_image_id)
                 for cdr_image_id in cdr_image_ids}

    missing_image_cdr_ids = [x for x in hash_dict if hash_dict[x] is None]
    if len(missing_image_cdr_ids) == 0:
        return [hash_dict[cdr_image_id] for cdr_image_id in cdr_image_ids]

    # Hit the CDR for original URLs and lookup ids for image_hash
    data_dict = cdr_fields_for_cdr_ids(missing_image_cdr_ids,
                                       ['crawl_data.image_id',
                                        'obj_stored_url'],
                                       es)
    for cdr_image_id in data_dict:
        if 'crawl_data.image_id' in data_dict[cdr_image_id]:
            hash_dict[cdr_image_id] =\
                image_hash_for_memex_ht_id(
                    data_dict[cdr_image_id]['crawl_data.image_id'])

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
    Given a list of CDR IDs of advertisements, find their DD IDs and when they were posted.

    :param list cdr_ad_ids: A list of CDR IDs of escort ads.
    :returns: `pandas.DataFrame` -- DataFrame of CDR IDs, DD IDs, and Post Dates
    """
    return df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_post_date'])


def post_dates_for_hashed_cdr_image_id(cdr_image_id, es=None):
    """
    Given the ID of an image in the CDR, hash it, and get all of the dates \
    on which it was posted.

    :param str cdr_image_id: An image's CDR ID
    :param elasticsearch.Elasticsearch es: CDR Connection (can be omitted)
    :returns: `pandas.DataFrame` -- DataFrame of CDR IDs, Deep Dive IDs, and \
    Post Dates
    """
    cdr_ad_ids = cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id, es)
    if isinstance(cdr_ad_ids, (str, unicode)):
        cdr_ad_ids = [cdr_ad_ids]
    return post_dates_for_cdr_ad_ids(cdr_ad_ids)


def df_of_tables_for_cdr_image_ids(cdr_image_ids, dd_tables):
    """
    :param list cdr_image_ids: List of CDR IDs for images.
    :param list dd_tables: List of target SQLite / Deep Dive tables.
    :returns: `pandas.DataFrame` -- DataFrame of CDR Image IDS, CDR Ad IDS, DD IDs, and other desired tables.
    """
    cdr_ad_ids = cdr_ad_ids_for_cdr_image_ids(cdr_image_ids)
    ad_image_df = pd.DataFrame({'cdr_id': cdr_ad_ids, 'cdr_image_id': cdr_image_ids})
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, dd_tables)
    return ad_image_df.join(df, on='cdr_id')
