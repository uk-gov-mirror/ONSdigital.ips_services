import time
import uuid

import ips_common_db.sql as db
# noinspection PyUnresolvedReferences
import requests
from ips_common.ips_logging import log

from ips.services.dataimport.import_non_response import import_nonresponse
from ips.services.dataimport.import_shift import import_shift
from ips.services.dataimport.import_survey import import_survey
from ips.services.dataimport.import_traffic import import_sea, import_tunnel, import_air
from ips.services.dataimport.import_unsampled import import_unsampled

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


# noinspection PyUnusedLocal
def setup_module(module):
    """ setup any state specific to the execution of the given module."""
    log.info(f"run_id = {run_id}")
    import_reference_data()
    setup_pv()


def import_reference_data():
    log.info("-> Start data load")

    with open(survey_data, 'rb') as file:
        import_survey(run_id, file.read(), None, None)

    with open(reference_data['Sea'], 'rb') as file:
        import_sea(run_id, file.read(), None, None)

    with open(reference_data['Air'], 'rb') as file:
        import_air(run_id, file.read(), None, None)

    with open(reference_data['Tunnel'], 'rb') as file:
        import_tunnel(run_id, file.read(), None, None)

    with open(reference_data['Shift'], 'rb') as file:
        import_shift(run_id, file.read(), None, None)

    with open(reference_data['Non Response'], 'rb') as file:
        import_nonresponse(run_id, file.read(), None, None)

    with open(reference_data['Unsampled'], 'rb') as file:
        import_unsampled(run_id, file.read(), None, None)

    log.info("-> End data load")


def setup_pv():
    df = db.select_data('*', "PROCESS_VARIABLE_PY", 'RUN_ID', 'TEMPLATE')
    df['RUN_ID'] = run_id
    db.insert_dataframe_into_table('PROCESS_VARIABLE_PY', df)


# noinspection PyUnusedLocal
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
