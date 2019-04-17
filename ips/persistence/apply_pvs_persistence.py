import multiprocessing

import json
import numpy as np
import pandas as pd
from ips_common.ips_logging import log
from ips.persistence.pv_persistence import get_process_variables
from functools import partial

# for exec
import random
random.seed(123456)
import math

with open('../../data/steps_configuration.json') as config_file:
    STEP_CONFIG = json.load(config_file)


def order_to_execute_pvs():
    # TODO: Add order column to George's PV_BYTES table
    dict = {'shift_port_grp_pv': 1,
            'weekday_end_pv': 2,
            'am_pm_night_pv': 3,
            'shift_flag_pv': 4,
            'crossings_flag_pv': 5,
            'nr_port_grp_pv': 6,
            'mig_flag_pv': 7,
            'nr_flag_pv': 8,
            'mins_port_grp_pv': 9,
            'mins_ctry_grp_pv': 10,
            'mins_flag_pv': 11,
            'samp_port_grp_pv': 12,
            'unsamp_port_grp_pv': 13,
            'unsamp_region_grp_pv': 14,
            'imbal_port_grp_pv': 15,
            'imbal_ctry_fact_pv': 16,
            'imbal_port_fact_pv': 17,
            'imbal_eligible_pv': 18,
            'stay_imp_flag_pv': 19,
            'stay_imp_eligible_pv': 20,
            'stayimpctrylevel1_pv': 21,
            'stayimpctrylevel2_pv': 22,
            'stayimpctrylevel3_pv': 23,
            'stayimpctrylevel4_pv': 24,
            'stay_purpose_grp_pv': 25,
            'fares_imp_flag_pv': 26,
            'fares_imp_eligible_pv': 27,
            'discnt_f1_pv': 28,
            'discnt_package_cost_pv': 29,
            'discnt_f2_pv': 30,
            'fage_pv': 31,
            'type_pv': 32,
            'opera_pv': 33,
            'ukport1_pv': 34,
            'ukport2_pv': 35,
            'ukport3_pv': 36,
            'ukport4_pv': 37,
            'osport1_pv': 38,
            'osport2_pv': 39,
            'osport3_pv': 40,
            'osport4_pv': 41,
            'apd_pv': 42,
            'qmfare_pv': 43,
            'duty_free_pv': 44,
            'spend_imp_eligible_pv': 45,
            'spend_imp_flag_pv': 46,
            'uk_os_pv': 47,
            'pur1_pv': 48,
            'pur2_pv': 49,
            'pur3_pv': 50,
            'dur1_pv': 51,
            'dur2_pv': 52,
            'rail_cntry_grp_pv': 53,
            'rail_exercise_pv': 54,
            'rail_imp_eligible_pv': 55,
            'purpose_pv': 56,
            'reg_imp_eligible_pv': 57,
            'town_imp_eligible_pv': 58}
    return dict


def get_pv_list(run_id=None, reference_data=False, reference_data_name=None):
    pv_json = get_process_variables(run_id)
    pv_dataframe = pd.read_json(pv_json)

    if reference_data:
        pv_names = STEP_CONFIG[reference_data_name]['pv_columns2']
        pv_dataframe = pv_dataframe[pv_dataframe['PV_NAME'].isin(pv_names)]
    else:
        # TODO: When PROCESS_VARIABLE_PY is swapped out for PV_BYTES, the below PV_NAME will need attention. Rectified by
        #  adding ORDER column to PV_BYTES.
        for column_name, order in order_to_execute_pvs().items():
            pv_dataframe.loc[pv_dataframe['PV_NAME'] == column_name, 'ORDER'] = order

        pv_dataframe.sort_values(by='ORDER', inplace=True)
        pv_dataframe.drop(labels='ORDER', axis=1, inplace=True)

    return pv_dataframe.values.tolist()


def modify_values(row, pvs, dataset=None):
    """
    Author       : Thomas Mahoney
    Date         : 27 / 03 / 2018
    Purpose      : Applies the PV rules to the specified dataframe on a row by row basis.
    Parameters   : row - the row of a dataframe passed to the function through the 'apply' statement called
                   pvs - a collection of pv names and statements to be applied to the dataframe's rows.
                   dataset -  and identifier used in the executed pv statements.
    Returns      : a modified row to be reinserted into the dataframe.
    Requirements : this function must be called through a pandas apply statement.
    """

    for pv in pvs:
        code = pv[1]
        try:
            exec(code)
        except ValueError:
            log.error(f"ValueError on PV: {pv[0]}, code: {code}")
            raise ValueError

        except KeyError:
            log.error(f"KeyError on PV: {pv[0]}, code: {code}")
            raise KeyError

        except TypeError:
            log.error(f"TypeError on PV: {pv[0]}, code: {code}")
            raise TypeError

        except SyntaxError:
            log.error(f"SyntaxError on PV: {pv[0]}, code: {code}")
            raise SyntaxError

    if dataset in ('survey', 'shift'):
        row['SHIFT_PORT_GRP_PV'] = str(row['SHIFT_PORT_GRP_PV'])[:10]

    return row


def parallel_func(pv_df, pv_list, dataset=None):
    return pv_df.apply(modify_values, axis=1, args=(pv_list, dataset))


def parallelise_pvs(dataframe, pv_list, dataset=None):
    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(dataframe, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    res = pd.concat(
        pool.map(
            partial(parallel_func, pv_list=pv_list, dataset=dataset),
            df_split
        ),
        sort=True
    )

    pool.close()
    pool.join()

    return res
