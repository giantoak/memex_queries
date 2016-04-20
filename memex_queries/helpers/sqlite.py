SQLITE_FILE = 'dd_dump.db'
local_sqlite = None


def new_sqlite_con():
    """

    :return sqlalchemy.create_engine:
    """
    from sqlalchemy import create_engine

    global local_sqlite
    if local_sqlite is None:
        local_sqlite = create_engine('s3:///{}'.format(SQLITE_FILE))

    return local_sqlite


def get_phones_for_dd_ids(dd_ids, sql_con=None):
    """

    :param list|set dd_ids:
    :return pandas.DataFrame:
    """
    from pandas import read_sql

    if sql_con is None:
        sql_con = new_sqlite_con()

    query = 'select * from dd_id_to_phone where dd_id in ({})'.format(','.join(str(x) for x in dd_ids))

    return read_sql(query, sql_con)


def dd_df_from_sqlite_tables(dd_ids, sqlite_tables, sql_con=None):
    """

    :param dd_ids:
    :param sqlite_tables:
    :return pandas.DataFrame:
    """
    from pandas import read_sql

    dd_ids_str = ','.join(str(x) for x in dd_ids)
    query_fmt = 'select * from {} where dd_id in ({})'.format

    if sql_con is None:
        sql_con = new_sqlite_con()

    df = read_sql(query_fmt(sqlite_tables[0], dd_ids_str), sql_con)

    for s_t in sqlite_tables[1:]:
        df_2 = read_sql(query_fmt(s_t, dd_ids_str), sql_con)
        df = df.merge(df_2, on=['dd_id'], how='outer')

    if 'post_date' in df:
        from pandas import to_datetime
        df['post_date'] = df.post_date.apply(to_datetime)

    return df