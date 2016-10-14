from ..memex.hbase import hbase_row_value_via_rest


def similar_images_for_cdr_image_id(cdr_image_id, with_scores=False):
    """
    :param str cdr_image_id: CDR ID of an image
    :param bool with_scores: if True, return
    :returns: `list[str]` | `dict[str, float]` -- list of CDR image IDs
    or dict keying CDR image IDs to similarity scores.
    """
    memex_ht_id = hbase_row_value_via_rest('ht_images_cdrid_to_image_ht_id',
                                           cdr_image_id,
                                           'info:crawl_data.memex_ht_id')

    return hbase_row_value_via_rest('aaron_memex_ht-images',
                                    memex_ht_id,
                                    'meta:columbia_near_dups')



def image_hash_for_cdr_image_id(cdr_image_id):
    """
    :param str cdr_image_id: CDR ID of an image
    :returns: `str` -- SHA1 Hash of image or None, if not in HBase
    """
    cdr_id_to_sha_tables = ['escorts_images_cdrid_infos'
                            'ht_images_cdrid_to_sha1_2016',
                            'ht_images_cdrid_to_sha1_2016_old_crawler',
                            'ht_images_cdrid_to_sha1_qpr_Apr2016_CP4',
                            'ht_images_cdrid_to_sha1_sample']

    for table in cdr_id_to_sha_tables:
        hash_str = hbase_row_value_via_rest(table,
                                            cdr_image_id,
                                            'hash:sha1')
        if hash_str is not None:
            return hash_str

    return None


def cdr_ad_ids_for_image_hash(image_hash):
    """
    :param image_hash: SHA1 hash of the image whose parents should be found
    :returns: `list` -- List of CDR IDs of ads using image, or None
    """
    ad_ids = hbase_row_value_via_rest('ht_images_infos_2016',
                                      image_hash,
                                      'info:all_parent_ids')

    if ad_ids is not None:
        return ad_ids.split(',')

    return None
