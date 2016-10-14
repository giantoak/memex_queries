HBASE_ADDR = 'memex-hbase-master'

local_hbase = None


def _new_hbase():
    import happybase
    global local_hbase
    if local_hbase is None:
        local_hbase = happybase.Connection(HBASE_ADDR, port=HBASE_PORT)

    return local_hbase


def hbase_row_value_via_rest(table_name, row_id, key_id):
    import requests
    hbase_url = 'http://' + '/'.join(['{}:8080'.format(HBASE_ADDR),
                                      table_name,
                                      row_id,
                                      key_id])
    try:
        r = requests.get(hbase_url)
        if r.status_code == 200:
            try:
                return r.json()
            except:
                return r.text
    except:
        pass

    return None


def hbase_rows_values_via_rest(table_name, row_ids, key_ids):
    """

    :param str table_name: The name of the MEMEX HBase table
    :param str row_id: The row to get from the table
    :param str key_id: The key to get from the row
    :returns: `list[list]` -- Lists of list of keys for rows
    """
    if not isinstance(row_ids, list):
        row_ids = [row_ids]

    if not isinstance(key_ids, list):
        key_ids = [key_ids]

    results = []
    for row_id in row_ids:
        results.append([hbase_row_value_via_rest(table_name,
                                                 row_id,
                                                 key_id)
                        for key_id in key_ids])

    return results


def hbase_rows_values_via_thrift(table_name, row_ids, key_ids=None, hb_con=None):
    """

    :param table_name:
    :param row_ids:
    :param key_ids:
    :param hb_con:
    :return:
    """
    if hb_con is None:
        hb_con = _new_hbase()

    if not isinstance(row_ids, list):
        row_ids = [row_ids]

    if key_ids is not None and not isinstance(key_ids, list):
        key_ids = [key_ids]

    tbl_con = hb_con.table(table_name)

    return tbl_con.rows(rows=row_ids, columns=key_ids)


def hbase_rows_values(table_name,
                      row_ids,
                      key_ids=None,
                      con_type='thrift',
                      hb_con=None):
    """

    :param table_name:
    :param row_ids:
    :param key_ids:
    :param con_type:
    :param hb_con:
    :return:
    """

    con_type = con_type.lower()
    if con_type not in ['thrift', 'rest']:
        raise ValueError('con_type = {}, expected "thrift" or "rest"'.format(con_type))

    if con_type == 'thrift':
        return hbase_rows_values_via_thrift(table_name, row_ids, key_ids, hb_con)

    if key_ids is None:
        raise ValueError('must specify HBase families if using the REST interface')

    return hbase_rows_values_via_rest(table_name, row_ids, key_ids)
