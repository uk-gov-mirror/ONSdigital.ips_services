import multiprocessing
from functools import partial

import ips.persistence.persistence as db
# noinspection PyUnresolvedReferences
import math
import numpy as np
import pandas
from ips_common.ips_logging import logging as log

from ips.persistence.persistence import insert_from_dataframe


def modify_values(row, dataset, pvs):
    for x in pvs:
        code = pvs[x]
        try:
            exec(code)
        except ValueError:
            log.error(f"ValueError on PV: {x}")
            raise ValueError

        except KeyError:
            log.error(f"KeyError on PV: {x}")
            raise KeyError

        except TypeError:
            log.error(f"TypeError on PV: {x}")
            raise TypeError

        except SyntaxError:
            log.error(f"SyntaxError on PV: {x}")
            raise SyntaxError

    if dataset in ('survey', 'shift'):
        row['SHIFT_PORT_GRP_PV'] = str(row['SHIFT_PORT_GRP_PV'])[:10]

    return row


def get_pvs():
    return db.execute_sql()(
        "SELECT PROCVAR_NAME, PROCVAR_RULE FROM SAS_PROCESS_VARIABLE ORDER BY PROCVAR_ORDER"
    ).fetchall()


def parallel_func(pv_df, pv_list, dataset=None):
    out_dict = {x['PROCVAR_NAME']: compile(x['PROCVAR_RULE'], 'pv', 'exec') for x in pv_list}
    return pv_df.apply(modify_values, axis=1, args=(dataset, out_dict))


def parallelise_pvs(dataframe, process_variables, dataset=None):
    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(dataframe, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    res = pandas.concat(
        pool.map(
            partial(parallel_func, pv_list=process_variables, dataset=dataset),
            df_split
        ),
        sort=True
    )

    pool.close()
    pool.join()

    return res


def process(in_table_name, out_table_name, in_id, dataset):
    df_data = db.read_table_values(in_table_name)()
    df_data.fillna(value=np.NaN, inplace=True)

    process_variables = get_pvs()
    pvs = [dict(a.items()) for a in process_variables]

    if dataset == 'survey':
        df_data.sort_values('SERIAL', inplace=True)

    # Apply process variables
    df_data = parallelise_pvs(df_data, pvs, dataset)

    columns = [in_id.upper()] + [pv.values()[0].upper() for pv in process_variables]

    df_out = df_data[columns]

    insert_from_dataframe(out_table_name)(df_out)
