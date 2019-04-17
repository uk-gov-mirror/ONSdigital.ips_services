import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence import apply_pvs_persistence as run
from ips.persistence.persistence import read_table_values

NON_RESPONSE_DATA_TABLE = 'NON_RESPONSE_DATA'
get_non_response_data = read_table_values(NON_RESPONSE_DATA_TABLE)


def _get_non_response_data(run_id=None):
    nr_data = get_non_response_data()
    nr_data = nr_data.loc[nr_data['RUN_ID'] == run_id]
    nr_data.drop(labels=['RUN_ID', 'YEAR', 'MONTH', 'DATA_SOURCE_ID'], axis=1, inplace=True)

    return nr_data


def apply_pvs_to_non_response_data(run_id):
    # Get survey data
    nr_data = _get_non_response_data(run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='NON_RESPONSE')

    # Apply PVs to data
    data = run.parallelise_pvs(nr_data, pv_list)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_NON_RESPONSE_DATA', data)


if __name__ == '__main__':
    log.info("Start test")
    run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    delete_from_table(NON_RESPONSE_DATA_TABLE)()
    delete_from_table('SAS_NON_RESPONSE_DATA')()

    from ips.services.dataimport.import_non_response import import_nonresponse_file
    df = import_nonresponse_file(run_id, '../../tests/data/import_data/dec/Dec17_NR.csv')

    apply_pvs_to_non_response_data(run_id)
    log.info("End test")
