HBASE_ADDR = 'memex-hbase-master:8080'


def _hbase_row_value(table, row_id, key_id):
    """
    :param str table: The name of the MEMEX HBase table
    :param str row_id: The row to get from the table
    :param str key_id: The key to get from the row
    :returns: `str` -- The value in the desired key, or `None`
    """
    import requests
    try:
        hbase_url = 'http://{}/{}/{}/{}'.format(HBASE_ADDR, table, row_id, key_id)
        r = requests.get(hbase_url)
        if r.status_code == 200:
            return r.text
    except:
        pass

    return None


def image_hash_for_cdr_image_id(cdr_image_id):
    """
    :param cdr_image_id:
    :returns: `str` -- SHA1 Hash of image or None, if not in HBase
    """
    cdr_id_to_sha_tables = ['ht_images_cdrid_to_sha1_2016',
                            'ht_images_cdrid_to_sha1_2016_old_crawler',
                            'ht_images_cdrid_to_sha1_qpr_Apr2016_CP4',
                            'ht_images_cdrid_to_sha1_sample']

    for table in cdr_id_to_sha_tables:
        hash_str = _hbase_row_value(table, cdr_image_id, 'hash:sha1')
        if hash_str is not None:
            return hash_str

    return None


def cdr_ad_ids_for_image_hash(image_hash):
    """
    :param image_hash: SHA1 hash of the image whose parents should be found
    :returns: `list` -- List of CDR IDs of ads using image, or None
    """
    ad_ids = _hbase_row_value('ht_images_infos_2016',
                              image_hash,
                              'info:all_parent_ids')

    if ad_ids is not None:
        return ad_ids.split(',')

    return None