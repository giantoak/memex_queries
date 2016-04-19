HBASE_URL = 'memex-hbase-master:8080'


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
    Query HBase to to get the Lattice / Deep Dive Dump Ad ID that maps to the corresponding CDR Ad ID
    :param str cdr_id: The CDR ID of a scraped escort ad.
    :return str:
    """
    return hbase_row_value('cdr_id_to_dd_id', cdr_id, 'info:dd_id')


def dd_id_df(cdr_ids):
    """
    Return a dataframe mapping a list of CDR Ad IDs to their corresponding DD IDs.
    :param cdr_ids:
    :return pandas.DataFrame:
    """
    from pandas import DataFrame
    return DataFrame({'cdr_id': cdr_ids,
                      'dd_id': [dd_id(cdr_id) for cdr_id in cdr_ids]})