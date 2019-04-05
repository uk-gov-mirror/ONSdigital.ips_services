import os
import time

import ips_common_db.sql as db
import pandas as pd

from ips.persistence import data_management as idm
from ips.services.calculations import calculate_traffic_weight as tr_calc
from ips.services.dataimport import CSVType
from ips.services.dataimport.import_non_response import import_non_response
from ips.services.dataimport.import_shift import import_shift
from ips.services.dataimport.import_survey import import_survey_file
from ips.services.dataimport.import_traffic import import_traffic
from ips.services.dataimport.import_unsampled import import_unsampled


def import_survey_data_into_database(survey_data_path, run_id):
    """
    Author       : (pinched from) Thomas Mahoney (modified by) Elinor Thorne
    Date         : (26/04/ 2018) 23/08/2018
    Purpose      : Loads the dataimport data into 'SURVEY_SUBSAMPLE' table on the connected database.
    Parameters   : survey_data_path - the dataframe containing all of the dataimport data.
    Returns      : NA
    Requirements : Datafile is of type '.csv', '.pkl' or '.sas7bdat'
    """

    start_time = time.time()

    # Check the survey_data_path's suffix to see what it matches then extract using the appropriate method.
    # TODO:
    # fares_imputation originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # imbalance_weight originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # rail_imputation originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # spend_imputation originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # town_and_stay originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # unsampled_weight originally had: df_survey_data = pd.read_csv(survey_data_path, encoding='ANSI', dtype=str)
    # final_weight originally had: df_survey_data = pd.read_csv(survey_data_path)

    # df_survey_data = pd.read_csv(survey_data_path, engine='python')
    # TODO: Swap with reusable function
    df_survey_data = pd.read_csv(survey_data_path, engine='python')

    # Add the generated run id to the dataset.
    df_survey_data['RUN_ID'] = pd.Series(run_id, index=df_survey_data.index)

    # Cleanses Survey Subsample table.
    db.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)

    # Insert the imported data into the survey_subsample table on the database.
    # fast=False causes arithmetic error
    db.insert_dataframe_into_table(idm.SURVEY_SUBSAMPLE_TABLE, df_survey_data)

    # Print Import runtime to record performance.
    print("Import runtime: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))


def import_test_data_into_database(import_data_dir, run_id, load_survey_data=True):
    """
    This function prepares all the data necessary to run all 14 steps for testing.
    Note that no process variables are uploaded and are expected to be present in the database.

    load_survey_data is set to True as 2 of 14 tests were written to use different survey data
    """

    # Import data paths (these will be passed in through the user)
    shift_data_path = os.path.join(import_data_dir, 'Poss shifts Dec 2017.csv')
    nr_data_path = os.path.join(import_data_dir, 'Dec17_NR.csv')
    sea_data_path = os.path.join(import_data_dir, 'Sea Traffic Dec 2017.csv')
    tunnel_data_path = os.path.join(import_data_dir, 'Tunnel Traffic Dec 2017.csv')
    air_data_path = os.path.join(import_data_dir, 'Air Sheet Dec 2017 VBA.csv')
    unsampled_data_path = os.path.join(import_data_dir, 'Unsampled Traffic Dec 2017.csv')

    # Cleanse
    db.delete_from_table(idm.SAS_SURVEY_SUBSAMPLE_TABLE)

    if load_survey_data:
        db.delete_from_table(idm.SURVEY_SUBSAMPLE_TABLE, 'RUN_ID', '=', run_id)
        survey_data_path = os.path.join(import_data_dir, 'surveydata.csv')
        import_survey_file(survey_data_path=survey_data_path, run_id=run_id)

    import_traffic(file_type=CSVType.Sea, run_id=run_id, file_name=sea_data_path)
    import_traffic(file_type=CSVType.Air, run_id=run_id, file_name=air_data_path)
    import_traffic(file_type=CSVType.Tunnel, run_id=run_id, file_name=tunnel_data_path)

    import_shift(file_type=CSVType.Shift, run_id=run_id, file_name=shift_data_path)
    import_non_response(file_type=CSVType.NonResponse, run_id=run_id, file_name=nr_data_path)
    import_unsampled(file_type=CSVType.Unsampled, run_id=run_id, file_name=unsampled_data_path)


def populate_test_pv_table(conn, run_id, pv_run_id):
    """ Set up table to run and test copy_step_pvs_for_survey_data() """

    db.delete_from_table('PROCESS_VARIABLE_TESTING', 'RUN_ID', '=', run_id)
    db.delete_from_table('PROCESS_VARIABLE_PY', 'RUN_ID', '=', run_id)

    sql1 = f"""
        INSERT INTO PROCESS_VARIABLE_TESTING
        SELECT * FROM PROCESS_VARIABLE_PY
        WHERE RUN_ID = '{pv_run_id}'
    """

    sql2 = f"""
        UPDATE PROCESS_VARIABLE_TESTING
        SET RUN_ID = '{run_id}'
        WHERE RUN_ID = '{pv_run_id}'
    """

    sql3 = f"""
        INSERT INTO PROCESS_VARIABLE_PY
        SELECT * FROM PROCESS_VARIABLE_TESTING
        WHERE RUN_ID = '{run_id}'
    """

    conn.engine.execute(sql1)
    conn.engine.execute(sql2)
    conn.engine.execute(sql3)


def reset_test_tables(run_id, step_config):
    """ Cleanses tables within database. """

    # Clear tables unique to Traffic Weight step
    if step_config['name'] == 'TRAFFIC_WEIGHT':
        # clear the input SQL server tables for the step
        db.delete_from_table(tr_calc.POP_TOTALS)

        # clear the auxillary tables
        db.delete_from_table(tr_calc.SURVEY_TRAFFIC_AUX_TABLE)

        # drop aux tables and r created tables
        db.drop_table(tr_calc.POP_PROWVEC_TABLE)
        db.drop_table(tr_calc.R_TRAFFIC_TABLE)

    # List of tables to cleanse entirely.
    tables_to_unconditionally_cleanse = [idm.SAS_SURVEY_SUBSAMPLE_TABLE,
                                         idm.SAS_PROCESS_VARIABLES_TABLE]

    if 'spv_table' in step_config:
        tables_to_unconditionally_cleanse.append(step_config['spv_table'])

    if 'data_table' in step_config:
        tables_to_unconditionally_cleanse.append(step_config['data_table'])

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list.
    for table in tables_to_unconditionally_cleanse:
        try:
            db.delete_from_table(table)
        except Exception:
            continue

    # List of tables to cleanse where [RUN_ID] = RUN_ID.
    tables_to_cleanse = ['PROCESS_VARIABLE_PY', 'PROCESS_VARIABLE_TESTING']

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list.
    for table in tables_to_cleanse:
        try:
            db.delete_from_table(table, 'RUN_ID', '=', run_id)
        except Exception:
            continue

    # Try to delete from each table in list.  If exception occurs, assume table is
    # already empty, and continue deleting from tables in list.
    for table in step_config['delete_tables']:
        try:
            db.delete_from_table(table)
        except Exception:
            continue


def populate_test_data(table_name, run_id, step_config, dataset):
    step_name = step_config['name'].lower()
    test_dir = os.path.join(r'data/main/dec/new_test', step_name)
    file_name = dataset + '_out_expected.csv'

    expect_df = db.select_data('*', table_name, 'RUN_ID', run_id)

    # Cols
    if dataset == 'survey':
        cols = step_config['nullify_pvs']
        cols = [item.replace('[', '') for item in cols]
        cols = [item.replace(']', '') for item in cols]
        cols.insert(0, 'SERIAL')
        expect_df = expect_df[cols]
        file_name = dataset + 'data_out_expected.csv'

    try:
        expect_df.to_csv(os.path.join(test_dir, file_name), index=False)
    except FileNotFoundError:
        os.mkdir(test_dir)
        expect_df.to_csv(os.path.join(test_dir, file_name), index=False)
