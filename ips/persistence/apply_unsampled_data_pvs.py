import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence import apply_pvs_persistence as run


def apply_pvs_to_unsamp_data(run_id, dataset):
    unsampled_data_table = 'UNSAMPLED_OOH_DATA'

    # Get survey data
    data = run.get_reference_data(unsampled_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='unsampled_weight')

    # Apply PVs to data
    data = run.parallelise_pvs(data, pv_list, dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_UNSAMPLED_OOH_DATA', data)


if __name__ == '__main__':
    log.info("Start test")
    run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    delete_from_table('UNSAMPLED_OOH_DATA')()
    delete_from_table('SAS_UNSAMPLED_OOH_DATA')()

    from ips.services.dataimport.import_unsampled import import_unsampled_file
    df = import_unsampled_file(run_id, '../../tests/data/import_data/dec/Unsampled Traffic Dec 2017.csv')

    apply_pvs_to_unsamp_data(run_id, dataset='unsampled')
    log.info("End test")
