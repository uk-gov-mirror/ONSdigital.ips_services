import numpy as np
import io
import pandas
import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence.persistence import read_table_values
from ips.persistence.pv_persistence import get_process_variables

# for exec
import random
random.seed(123456)
import math

PROCESS_VARIABLES_TABLE = 'PROCESS_VARIABLE_PY'
PV_CODE = 'PV_DEF'

SURVEY_SUBSAMPLE_TABLE = 'SURVEY_SUBSAMPLE'

get_pv = read_table_values(PROCESS_VARIABLES_TABLE)
get_survey_subsample = read_table_values(SURVEY_SUBSAMPLE_TABLE)


def _order_to_execute_pvs():
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
            'mins_nat_grp_pv': 11,
            'mins_flag_pv': 12,
            'samp_port_grp_pv': 13,
            'unsamp_port_grp_pv': 14,
            'unsamp_region_grp_pv': 15,
            'imbal_port_grp_pv': 16,
            'imbal_ctry_fact_pv': 17,
            'imbal_port_fact_pv': 18,
            'imbal_eligible_pv': 19,
            'stay_imp_flag_pv': 20,
            'stay_imp_eligible_pv': 21,
            'stayimpctrylevel1_pv': 22,
            'stayimpctrylevel2_pv': 23,
            'stayimpctrylevel3_pv': 24,
            'stayimpctrylevel4_pv': 25,
            'stay_purpose_grp_pv': 26,
            'fares_imp_flag_pv': 27,
            'fares_imp_eligible_pv': 28,
            'discnt_f1_pv': 29,
            'discnt_package_cost_pv': 30,
            'discnt_f2_pv': 31,
            'fage_pv': 32,
            'type_pv': 33,
            'opera_pv': 34,
            'ukport1_pv': 35,
            'ukport2_pv': 36,
            'ukport3_pv': 37,
            'ukport4_pv': 38,
            'osport1_pv': 39,
            'osport2_pv': 40,
            'osport3_pv': 41,
            'osport4_pv': 42,
            'apd_pv': 43,
            'qmfare_pv': 44,
            'duty_free_pv': 45,
            'spend_imp_eligible_pv': 46,
            'spend_imp_flag_pv': 47,
            'uk_os_pv': 48,
            'pur1_pv': 49,
            'pur2_pv': 50,
            'pur3_pv': 51,
            'dur1_pv': 52,
            'dur2_pv': 53,
            'rail_cntry_grp_pv': 54,
            'rail_exercise_pv': 55,
            'rail_imp_eligible_pv': 56,
            'reg_imp_eligible_pv': 57,
            'town_imp_eligible_pv': 58}
    return dict


def _get_pv_list(run_id=None):
    pv_json = get_process_variables(run_id)
    pv_dataframe = pandas.read_json(pv_json)

    # TODO: When PROCESS_VARIABLE_PY is swapped out for PV_BYTES, the below PV_NAME will need attention. Rectified by
    #  adding ORDER column to PV_BYTES.
    for column_name, order in _order_to_execute_pvs().items():
        pv_dataframe.loc[pv_dataframe['PV_NAME'] == column_name, 'ORDER'] = order

    pv_dataframe.sort_values(by='ORDER', inplace=True)
    pv_dataframe.drop(labels='ORDER', axis=1, inplace=True)

    return pv_dataframe.values.tolist()


def _get_survey_data(run_id=None):
    survey_data = get_survey_subsample()
    survey_data = survey_data.loc[survey_data['RUN_ID'] == run_id]
    survey_data.drop(labels='RUN_ID', axis=1, inplace=True)
    survey_data.fillna(value=np.NaN, inplace=True)
    survey_data.sort_values('SERIAL', inplace=True)

    return survey_data


def _modify_values(row, pvs, dataset):
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


def apply_pvs_to_survey_data(run_id):
    # Get survey data
    survey_data = _get_survey_data(run_id)

    # Get process variables
    pv_list = _get_pv_list(run_id='TEMPLATE')

    # Apply process variables
    dataset = 'survey'
    data = survey_data.apply(_modify_values, axis=1, args=(pv_list, dataset))

    if 'IND' in data.columns:
        data.drop(labels='IND', axis=1, inplace=True)

    if 'REGION' in data.columns:
        data.drop(labels='REGION', axis=1, inplace=True)

    # Insert the dataframe to SURVEY_SUBSAMPLE
    db.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', data)


if __name__ == '__main__':
    run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    cleanse = delete_from_table('SURVEY_SUBSAMPLE')
    cleanse()

    from ips.persistence.import_survey import import_survey_from_file
    import_survey_from_file(run_id, r'/Users/ThornE1/PycharmProjects/ips_services/tests/data/import_data/dec/survey_data_in_actual.csv')

    apply_pvs_to_survey_data(run_id)
