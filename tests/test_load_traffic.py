import time
import uuid
import falcon

import ips_common_db.sql as db
from ips_common.ips_logging import log
from ips.services.dataimport.import_traffic import import_air, import_sea, import_tunnel
from ips.persistence.import_traffic import TRAFFIC_TABLE

import pytest

tunnel_data = "data/import_data/dec/Tunnel Traffic Dec 2017.csv"
sea_data = "data/import_data/dec/Sea Traffic Dec 2017.csv"
air_data = "data/import_data/dec/Air Sheet Dec 2017 VBA.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


# noinspection PyUnusedLocal
def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_month1():
    month = '25'
    year = '2009'
    with open(tunnel_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_tunnel(run_id, file.read(), month, year)
    with open(sea_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_sea(run_id, file.read(), month, year)
    with open(air_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_air(run_id, file.read(), month, year)


def test_month2():
    # matching year, invalid month
    month = '11'
    year = '2017'
    with open(tunnel_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_tunnel(run_id, file.read(), month, year)
    with open(sea_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_sea(run_id, file.read(), month, year)
    with open(air_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_air(run_id, file.read(), month, year)


def test_year1():
    # valid month, data not matching valid year
    month = '12'
    year = '2009'
    with open(tunnel_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_tunnel(run_id, file.read(), month, year)
    with open(sea_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_sea(run_id, file.read(), month, year)
    with open(air_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_air(run_id, file.read(), month, year)


def test_invalid_quarter():
    # valid month, data not matching valid year
    month = 'Q5'
    year = '2009'
    with open(tunnel_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_tunnel(run_id, file.read(), month, year)
    with open(sea_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_sea(run_id, file.read(), month, year)
    with open(air_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_air(run_id, file.read(), month, year)

def test_valid_quarter():
    # valid month, data not matching valid year
    month = 'Q4'
    year = '2017'
    with open(tunnel_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_tunnel(run_id, file.read(), month, year)
    with open(sea_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_sea(run_id, file.read(), month, year)
    with open(air_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_air(run_id, file.read(), month, year)


def test_valid_import():
    with open(tunnel_data, 'rb') as file:
        import_tunnel(run_id, file.read(), '12', '2017')
    with open(sea_data, 'rb') as file:
        import_sea(run_id, file.read(), '12', '2017')
    with open(air_data, 'rb') as file:
        import_air(run_id, file.read(), '12', '2017')


# noinspection PyUnusedLocal
def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(TRAFFIC_TABLE, 'RUN_ID', '=', run_id)
