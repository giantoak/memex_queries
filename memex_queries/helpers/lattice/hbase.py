HBASE_ADDR = 'memex-hbase-master:8080'


def _hbase_lattice_value(cdr_id, key_id):
    """
    :param str cdr_id: The row to get from the table
    :param str key_id: The key to get from the row
    :returns: `list[str]` -- The value in the desired key or 'None'
    """
    import requests
    try:
        hbase_url = 'http://{}/{}/{}/{}:results'.format(HBASE_ADDR,
                                                        'lattice_hdfs',
                                                        cdr_id,
                                                        key_id)
        r = requests.get(hbase_url)
        if r.status_code == 200:
            try:
                return r.json()
            except:
                return r.text()

    except:
        return None

    return None
