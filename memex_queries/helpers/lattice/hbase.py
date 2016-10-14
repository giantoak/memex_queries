from ..memex.hbase import hbase_rows_values
import pandas as pd


def get_lattice_df(cdr_ids, key_ids, flatten=True):
    """

    :param list cdr_ids: List of CDR IDs
    :param list key_ids: List of target keys
    :param bool flatten: if True, create multiple rows when a cell
    has multiple values.
    :return: `pandas.DataFrame` -- dataframe of retrieved values
    """

    if not isinstance(list, cdr_ids):
        cdr_ids = [cdr_ids]

    if not isinstance(list, key_ids):
        key_ids = ['{}:values'.format(key_ids)]
    else:
        key_ids = ['{}:values'.format(key_id) for key_id in key_ids]

    results = hbase_rows_values('lattice_hdfs', cdr_ids, key_ids)

    new_results = []
    i = 0
    while i < len(results):
        new_results.append({})
        new_results[-1]['cdr_id'] = results[i][0]
        for key in results[i][1]:
            new_key = key.split(':')[0]
            new_results[-1][new_key] = results[i][1][key]
            if flatten is True:
                if isinstance(new_results[-1][new_key], list):
                    if len(new_results[-1][new_key]) == 1:
                        new_results[-1][new_key] = new_results[-1][new_key][0]
                    else:
                        for j in range(1, len(new_results[-1][new_key])):
                            results.append(results[i].copy())
                            results[-1][1][key] = new_results[-1][new_key][j]

                        new_results[-1][new_key] = new_results[-1][new_key][0]

    return pd.DataFrame(new_results).drop_duplicates()


