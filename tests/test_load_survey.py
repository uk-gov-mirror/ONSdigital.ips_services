import time
import uuid

import falcon
import ips_common_db.sql as db
from ips_common.ips_logging import log

from ips.persistence.import_survey import SURVEY_SUBSAMPLE
from ips.services.dataimport.import_survey import import_survey

import pytest

survey_data = "data/import_data/dec/surveydata.csv"

run_id = str(uuid.uuid4())
start_time = time.time()


# noinspection PyUnusedLocal
def setup_module(module):
    log.info("Module level start time: {}".format(start_time))


def test_month1():
    with open(survey_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_survey(run_id, file.read(), '25', '2009')


def test_month2():
    # matching year, invalid month
    with open(survey_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_survey(run_id, file.read(), '11', '2017')


def test_year1():
    # valid month, data not matching valid year
    with open(survey_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_survey(run_id, file.read(), '12', '2009')


def test_invalid_quarter():
    # valid month, data not matching valid year
    with open(survey_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_survey(run_id, file.read(), 'Q5', '2009')


def test_valid_quarter():
    # valid month, data not matching valid year
    with open(survey_data, 'rb') as file:
        with pytest.raises(falcon.HTTPError) as e_info:
            import_survey(run_id, file.read(), 'Q4', '2017')


def test_valid_import():
    with open(survey_data, 'rb') as file:
        import_survey(run_id, file.read(), '12', '2017')


# noinspection PyUnusedLocal
def teardown_module(module):
    log.info("Duration: {}".format(time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
    db.delete_from_table(SURVEY_SUBSAMPLE, 'RUN_ID', '=', run_id)
