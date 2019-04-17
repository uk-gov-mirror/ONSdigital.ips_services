import numpy as np
import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence import apply_pvs_persistence as run
from ips.persistence.persistence import read_table_values

SURVEY_SUBSAMPLE_TABLE = 'SURVEY_SUBSAMPLE'
get_survey_subsample = read_table_values(SURVEY_SUBSAMPLE_TABLE)


def _get_survey_data(run_id=None):
    survey_data = get_survey_subsample()
    survey_data = survey_data.loc[survey_data['RUN_ID'] == run_id]
    survey_data.drop(labels='RUN_ID', axis=1, inplace=True)
    survey_data.fillna(value=np.NaN, inplace=True)
    survey_data.sort_values('SERIAL', inplace=True)

    return survey_data


def apply_pvs_to_survey_data(run_id):
    # Get survey data
    survey_data = _get_survey_data(run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE')

    # Apply process variables
    dataset = 'survey'
    data = run.parallelise_pvs(survey_data, pv_list, dataset)

    # Cleanse
    if 'IND' in data.columns:
        data.drop(labels='IND', axis=1, inplace=True)

    if 'REGION' in data.columns:
        data.drop(labels='REGION', axis=1, inplace=True)

    # Insert the dataframe to SURVEY_SUBSAMPLE
    db.insert_dataframe_into_table('SAS_SURVEY_SUBSAMPLE', data)


if __name__ == '__main__':
    log.info("Start test")
    run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    delete_from_table('SURVEY_SUBSAMPLE')()
    delete_from_table('SAS_SURVEY_SUBSAMPLE')()

    from ips.services.dataimport.import_survey import import_survey_file
    df = import_survey_file(run_id, '../../tests/data/import_data/dec/survey_data_in_actual.csv')

    apply_pvs_to_survey_data(run_id)
    log.info("End test")
