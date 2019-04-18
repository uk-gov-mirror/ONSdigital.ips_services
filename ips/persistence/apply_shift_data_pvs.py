import ips_common_db.sql as db
from ips.persistence import apply_pvs_persistence as run

shift_data_table = 'SHIFT_DATA'


def apply_pvs_to_shift_data(run_id, dataset):
    # Get reference data
    data = run.get_reference_data(shift_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='shift_weight')

    # Apply PVs to data
    # dataset = 'shift'
    data = run.parallelise_pvs(data, pv_list, dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_SHIFT_DATA', data)
