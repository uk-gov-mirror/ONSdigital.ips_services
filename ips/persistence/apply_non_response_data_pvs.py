import ips_common_db.sql as db
from ips_common.ips_logging import log

from ips.persistence import apply_pvs_persistence as run

non_response_data_table = 'NON_RESPONSE_DATA'


def apply_pvs_to_non_response_data(run_id, dataset=None):
    # TODO: This function can be used for Shift, NR and Unsamp
    delete_from_table(non_response_data_table)(run_id=run_id)

    # Get reference data
    nr_data = run.get_reference_data(non_response_data_table, run_id=run_id)

    # Get process variables
    pv_list = run.get_pv_list(run_id='TEMPLATE', reference_data=True, reference_data_name='non_response')

    # Apply PVs to data
    data = run.parallelise_pvs(nr_data, pv_list, dataset=dataset)

    # Insert the dataframe to database
    db.insert_dataframe_into_table('SAS_NON_RESPONSE_DATA', data)


if __name__ == '__main__':
    log.info("Start test")
    test_run_id = 'EL-TEST-123'

    from ips.persistence.persistence import delete_from_table

    delete_from_table('NON_RESPONSE_DATA')()
    delete_from_table('SAS_NON_RESPONSE_DATA')()

    from ips.services.dataimport.import_non_response import import_nonresponse_file

    df = import_nonresponse_file(test_run_id, '../../tests/data/import_data/dec/Dec17_NR.csv')

    apply_pvs_to_non_response_data(test_run_id)
    log.info("End test")
