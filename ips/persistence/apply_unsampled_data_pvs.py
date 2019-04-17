import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence import apply_pvs_persistence as run
from ips.persistence.persistence import read_table_values

UNSAMPLED_DATA_TABLE = 'UNSAMPLED_OOH_DATA'
get_unsampled_data = read_table_values(UNSAMPLED_DATA_TABLE)


def _get_unsamp_data(run_id=None):
    # TODO: This is similar to _get_non_response_data.  Can this be moved to apply_pvs_persistence?
    data = get_unsampled_data()
    data = data.loc[data['RUN_ID'] == run_id]
    data.drop(labels=['RUN_ID', 'YEAR', 'MONTH', 'DATA_SOURCE_ID'], axis=1, inplace=True)

    return data


def apply_pvs_to_unsamp_data(run_id):
    # Get survey data
    data = _get_unsamp_data(run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='unsampled_weight')

    # Apply PVs to data
    dataset = 'unsampled'
    data = run.parallelise_pvs(data, pv_list, dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_UNSAMPLED_OOH_DATA', data)


if __name__ == '__main__':
    log.info("Start test")
    run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    delete_from_table(UNSAMPLED_DATA_TABLE)()
    delete_from_table('SAS_UNSAMPLED_OOH_DATA')()

    from ips.services.dataimport.import_unsampled import import_unsampled_file
    df = import_unsampled_file(run_id, '../../tests/data/import_data/dec/Unsampled Traffic Dec 2017.csv')

    apply_pvs_to_unsamp_data(run_id)
    log.info("End test")
