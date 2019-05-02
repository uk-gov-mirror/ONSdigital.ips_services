import time
import uuid

import ips_common_db.sql as db
import requests
from ips_common.ips_logging import log

from ips.services.dataimport.import_non_response import import_nonresponse_file
from ips.services.dataimport.import_shift import import_shift_file
from ips.services.dataimport.import_survey import import_survey_file
from ips.services.dataimport.import_traffic import import_air_file, import_sea_file, import_tunnel_file
from ips.services.dataimport.import_unsampled import import_unsampled_file

reference_data = {
    "Sea": "../tests/data/import_data/dec/Sea Traffic Dec 2017.csv",
    "Air": "../tests/data/import_data/dec/Air Sheet Dec 2017 VBA.csv",
    "Tunnel": "../tests/data/import_data/dec/Tunnel Traffic Dec 2017.csv",
    "Shift": "../tests/data/import_data/dec/Poss shifts Dec 2017.csv",
    "Non Response": "../tests/data/import_data/dec/Dec17_NR.csv",
    "Unsampled": "../tests/data/import_data/dec/Unsampled Traffic Dec 2017.csv"
}

survey_data = "../tests/data/import_data/dec/surveydata.csv"

run_id = str(uuid.uuid4())
start_time = time.time()
log.info("Module level start time: {}".format(start_time))


def setup_module(module):
    """ setup any state specific to the execution of the given module."""
    log.info(f"run_id = {run_id}")
    import_reference_data()
    setup_pv()


def import_reference_data():
    log.info("-> Start data load")

    import_sea_file(run_id, reference_data['Sea'])
    import_air_file(run_id, reference_data['Air'])
    import_tunnel_file(run_id, reference_data['Tunnel'])

    import_shift_file(run_id, reference_data['Shift'])
    import_nonresponse_file(run_id, reference_data['Non Response'])
    import_unsampled_file(run_id, reference_data['Unsampled'])

    import_survey_file(run_id, survey_data)

    log.info("-> End data load")


def setup_pv():
    df = db.select_data('*', "PROCESS_VARIABLE_PY", 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
        method.
    """
    db.delete_from_table('SURVEY_SUBSAMPLE')

    # List of tables to cleanse where [RUN_ID] = RUN_ID
    tables_to_cleanse = [
        'PROCESS_VARIABLE_PY',
        'PROCESS_VARIABLE_TESTING',
        'TRAFFIC_DATA',
        'SHIFT_DATA',
        'NON_RESPONSE_DATA',
        'UNSAMPLED_OOH_DATA'
    ]

    # Try to delete from each table in list where condition.  If exception occurs,
    # assume table is already empty, and continue deleting from tables in list.
    for table in tables_to_cleanse:
        try:
            db.delete_from_table(table, 'RUN_ID', '=', run_id)
        except Exception:
            continue

    log.info("End test")


# def test_workflow():
#     endpoint = 'http://localhost:5000/ips-service/start/' + run_id
#     log.info(f"Starting request... {endpoint}")
#     r = requests.put(endpoint)
#
#     assert (r.status_code == 200)
#
#     status_endpoint = 'http://localhost:5000/ips-service/status/' + run_id
#
#     done = False
#     perc = 0
#
#     while not done:
#         r = requests.get(status_endpoint)
#         assert (r.status_code == 200)
#         result = r.json()
#         perc_done = result['percentage_done']
#         if perc_done != perc:
#             log.info(f"Percentage Done: {perc_done}")
#             perc = perc_done
#         if perc_done == 100:
#             done = True
#         else:
#             time.sleep(10)


def test_workflow():
    from ips.services import ips_workflow
    workflow = ips_workflow.IPSWorkflow()
    workflow.run_calculations(run_id)
