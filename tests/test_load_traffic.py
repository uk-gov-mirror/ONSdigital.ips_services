import time
import uuid

import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.services.dataimport.import_traffic import import_air_file, import_sea_file, import_tunnel_file
from ips.persistence.import_traffic import TRAFFIC_TABLE

tunnel_data = "data/import_data/dec/Tunnel Traffic Dec 2017.csv"
sea_data = "data/import_data/dec/Sea Traffic Dec 2017.csv"
air_data = "data/import_data/dec/Air Sheet Dec 2017 VBA.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_survey_import():
    log.info(f"-> Start traffic data load for run_id: {run_id}")
    air_df = import_air_file(run_id, air_data)
    tunnel_df = import_tunnel_file(run_id, tunnel_data)
    sea_df = import_sea_file(run_id, sea_data)
    log.info(f"-> End traffic data load. air: {len(air_df)} rows, tunnel: {len(tunnel_df)}, sea: {len(sea_df)}.")


def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(TRAFFIC_TABLE, 'RUN_ID', '=', run_id)
