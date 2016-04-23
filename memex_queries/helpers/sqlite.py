SQLITE_FILE = 'dd_dump.db'
local_sqlite = None


def new_sqlite_con():
    """
    :returns: `sqlalchemy.create_engine` -- Connection to the SQLite database.
    """
    from sqlalchemy import create_engine

    global local_sqlite
    if local_sqlite is None:
        local_sqlite = create_engine('sqlite:///{}'.format(SQLITE_FILE))

    return local_sqlite


def dd_df_from_sqlite_tables(dd_ids, sqlite_tables, sql_con=None):
    """
    :param list dd_ids: list of Deep Dive IDs to retrieve
    :param list sqlite_tables: list of SQLite tables to join
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- dataframe of tables, joined using the Deep \
    Dive IDs.
    """
    from pandas import read_sql

    dd_ids_str = ','.join(['"{}"'.format(x) for x in dd_ids])
    query_fmt = 'select * from {} where dd_id in ({})'.format

    if sql_con is None:
        sql_con = new_sqlite_con()

    df = read_sql(query_fmt(sqlite_tables[0], dd_ids_str), sql_con)
    df['dd_id'] = df.dd_id.astype(int)

    for s_t in sqlite_tables[1:]:
        df_2 = read_sql(query_fmt(s_t, dd_ids_str), sql_con)
        df_2['dd_id'] = df_2.dd_id.astype(int)
        df = df.merge(df_2, on=['dd_id'], how='outer')

    if 'post_date' in df:
        from pandas import to_datetime
        df['post_date'] = df.post_date.apply(to_datetime)

    return df


def cdr_df_from_sqlite_tables(cdr_ad_ids, sqlite_tables, sql_con=None):
    """
    :param list cdr_ad_ids: list of CDR Ad IDs to match with deep dive data
    :param list sqlite_tables: list of SQLite tables to join
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- dataframe of tables, joined using the Deep \
    Dive IDs.
    """
    from pandas import read_sql

    cdr_ids_str = ','.join(['"{}"'.format(cdr_id) for cdr_id in cdr_ad_ids])

    if sql_con is None:
        sql_con = new_sqlite_con()

    df = read_sql('select * from dd_id_to_cdr_id where cdr_id in ({})'.format(cdr_ids_str), sql_con)
    df_2 = dd_df_from_sqlite_tables(list(df.dd_id), [x for x in sqlite_tables if x != 'dd_id_to_cdr_id'], sql_con)

    return df.merge(df_2, on=['dd_id'], how='outer')


def get_phones_for_dd_ids(dd_ids, sql_con=None):
    """
    :param list dd_ids: List of Deep Dive IDs
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- Data Frame of Deep Dive IDs and the phone \
    numbers assigned to them.
    """
    return dd_df_from_sqlite_tables(dd_ids, ['dd_id_to_phone'], sql_con)