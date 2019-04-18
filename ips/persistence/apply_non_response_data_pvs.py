import ips_common_db.sql as db
from ips.persistence import apply_pvs_persistence as run


def apply_pvs_to_non_response_data(run_id, dataset=None):
    # TODO: This function can be used for Shift, NR and Unsamp
    non_response_data_table = 'NON_RESPONSE_DATA'

    # Get reference data
    nr_data = run.get_reference_data(non_response_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id=run_id, reference_data=True, reference_data_name='non_response')

    # Apply PVs to data
    data = run.parallelise_pvs(nr_data, pv_list, dataset=dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_NON_RESPONSE_DATA', data)
