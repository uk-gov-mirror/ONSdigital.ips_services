import ips_common_db.sql as db
from ips.persistence import apply_pvs_persistence as run


def apply_pvs_to_unsamp_data(run_id, dataset):
    unsampled_data_table = 'UNSAMPLED_OOH_DATA'

    # Get reference data
    data = run.get_reference_data(unsampled_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id=run_id, reference_data=True, reference_data_name='unsampled_weight')

    # Apply PVs to data
    data = run.parallelise_pvs(data, pv_list, dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_UNSAMPLED_OOH_DATA', data)
