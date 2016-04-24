HBASE_ADDR = 'memex-hbase-master:8080'


def hbase_row_value(table, row_id, key_id):
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


def dd_id_for_cdr_ad_id(cdr_ad_id):
    """
    :param str cdr_ad_id: The CDR ID of a scraped escort ad.
    :returns: `int` -- The Deep Dive ID that maps to the CDR ID
    """
    return int(hbase_row_value('cdr_id_to_dd_id', cdr_ad_id, 'info:dd_id'))


def cdr_ad_id_for_dd_id(dd_id):
    """
    **THIS SECTION OF THE HBASE TABLE IS INCOMPLETE. DO NOT USE THIS /
    FUNCTION YET.** Instead, reference the `dd_id_to_cdr_id` table in SQLite.

    :param int dd_id: Deep Dive ID of an escort ad.
    :returns: `str` -- The CDR ID that maps to the Lattice / Deep Dive Dump Ad.
    """
    return hbase_row_value('deepdive_escort_ads', str(dd_id), 'info:cdr_id')


def dd_id_df_for_cdr_ad_ids(cdr_ad_ids):
    """
    :param list cdr_ad_ids: A list of CDR IDs of ads.
    :returns: `pandas.DataFrame` -- A two column DataFrame containing \
    CDR IDs and DD IDs.

    """
    from pandas import DataFrame
    return DataFrame({'cdr_id': cdr_ad_ids,
                      'dd_id': [dd_id_from_cdr_id(cdr_ad_id)
                                for cdr_ad_id in cdr_ad_ids]
                      })
