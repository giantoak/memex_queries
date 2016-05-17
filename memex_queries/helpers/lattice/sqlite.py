SQLITE_FILE = 'dd_dump_v2.db'
local_sqlite = None

feature_mapping_dict = {'phone': 'dd_id_to_phone',
                        'post_date': 'dd_id_to_post_date'}

def _new_sqlite_con():
    """
    :returns: `sqlalchemy.create_engine` -- Connection to the SQLite database.
    """
    from sqlalchemy import create_engine

    global local_sqlite
    if local_sqlite is None:
        local_sqlite = create_engine('sqlite:///{}'.format(SQLITE_FILE))

    return local_sqlite


def _df_of_tables_for_dd_ids(dd_ids, sqlite_tables, sql_con=None):
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
        sql_con = _new_sqlite_con()

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


def _df_of_phones_for_dd_ids(dd_ids, sql_con=None):
    """
    :param list dd_ids: List of Deep Dive IDs
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- Data Frame of Deep Dive IDs and the phone \
    numbers assigned to them.
    """
    return _df_of_tables_for_dd_ids(dd_ids, ['dd_id_to_phone'], sql_con)


def _df_of_tables_for_cdr_ad_ids(cdr_ad_ids, sqlite_tables, sql_con=None):
    """
    :param unicode|str|list cdr_ad_ids: list of CDR Ad IDs to match with deep dive data
    :param list sqlite_tables: list of SQLite tables to join
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- dataframe of tables, joined using the Deep \
    Dive IDs.
    """
    from pandas import read_sql

    if cdr_ad_ids is None:
        cdr_ad_ids = []

    if isinstance(cdr_ad_ids, (str, unicode)):
        cdr_ad_ids = [cdr_ad_ids]

    cdr_ids_str = ','.join(['"{}"'.format(cdr_id) for cdr_id in cdr_ad_ids])

    if sql_con is None:
        sql_con = _new_sqlite_con()

    df = read_sql('select * from dd_id_to_cdr_id where cdr_id in ({})'.format(cdr_ids_str), sql_con)
    df_2 = _df_of_tables_for_dd_ids(list(df.dd_id.unique()), [x for x in sqlite_tables if x != 'dd_id_to_cdr_id'],
                                    sql_con)

    return df.merge(df_2, on=['dd_id'], how='outer')


def df_of_features_for_cdr_ad_ids(cdr_ad_ids, features, sql_con=None):
    """
    :param unicode|str|list cdr_ad_ids: list of CDR Ad IDs to match with deep dive data
    :param list|str features: list of features to include, single feature if str or
    :param sqlalchemy.create_engine sql_con: Connection to SQLite (can be \
    omitted)
    :returns: `pandas.DataFrame` -- dataframe of tables, joined using the Deep \
    Dive IDs.
    """
    if isinstance(features, (str, unicode)):
        features = [features]

    for x in features:
        if x not in feature_mapping_dict:
            raise KeyError('No feature named "{}"'.format(x))

    return _df_of_tables_for_cdr_ad_ids(cdr_ad_ids,
                                        [feature_mapping_dict[x]
                                         for x in features],
                                        sql_con)

