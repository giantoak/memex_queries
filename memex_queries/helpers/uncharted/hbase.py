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


def image_hash_for_memex_ht_id(memex_ht_id):
    """
    :param memex_ht_id:
    :returns: `str` -- SHA1 Hash of image or None, if not in HBase
    """
    return _hbase_row_value('image_hash', memex_ht_id, 'image:hash')