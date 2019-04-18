import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.persistence import apply_pvs_persistence as run

shift_data_table = 'SHIFT_DATA'


def apply_pvs_to_shift_data(run_id, dataset):

    delete_from_table(shift_data_table)(run_id=run_id)
    # Get reference data
    data = run.get_reference_data(shift_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='shift_weight')

    # Apply PVs to data
    # dataset = 'shift'
    data = run.parallelise_pvs(data, pv_list, dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_SHIFT_DATA', data)


if __name__ == '__main__':
    log.info("Start test")
    test_run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table
    delete_from_table('SAS_SHIFT_DATA')()

    from ips.services.dataimport.import_shift import import_shift_file
    df = import_shift_file(test_run_id, '../../tests/data/import_data/dec/Poss shifts Dec 2017.csv')

    apply_pvs_to_shift_data(test_run_id, dataset='shift')
    log.info("End test")
