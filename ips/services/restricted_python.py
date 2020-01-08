import falcon

import ips.persistence.sql as db
from ips.util import process_variables as pv_exec
from ips.services.dataimport.import_survey import import_survey
from ips.services import service
import pandas as pd
import os


def get_dataframe() -> pd.DataFrame:
    survey_subsample_table = 'SURVEY_SUBSAMPLE'
    run_id = 'test-pv'
    month = '12'
    year = '2017'

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    input_survey_data = os.path.join(__location__, 'data/ips1712bv4_amtspnd.csv')

    db.delete_from_table(survey_subsample_table, 'RUN_ID', '=', run_id)

    # Load Survey data
    with open(input_survey_data, 'rb') as file:
        df = import_survey(run_id, file.read(), month, year)

    # Add PV's

    df['OSPORT2_PV'] = 0.0
    df['UNSAMP_PORT_GRP_PV'] = 0.0
    df['IMBAL_PORT_GRP_PV'] = 0.0
    df['SPEND'] = 0.0
    df['RAIL_EXERCISE_PV'] = 0.0
    df['UKPORT3_PV'] = 0.0
    df['UKPORT2_PV'] = 0.0
    df['UKPORT1_PV'] = 0.0
    df['STAYIMPCTRYLEVEL1_PV'] = 0.0
    df['STAY'] = 0.0
    df['IND'] = 0.0
    df['OSPORT3_PV'] = 0.0
    df['DISCNT_PACKAGE_COST_PV'] = 0.0
    df['DISCNT_F1_PV'] = 0.0

    return df


class Status(object):
    def __init__(self, pv_template):
        self.template = pv_template
        self.status = ""


class ErrorStatus(Status):
    def __init__(self, pv_template, message, pv):
        super().__init__(pv_template)
        self.status = "error"
        self.errorMessage = message
        self.PV = pv


class SuccessfulStatus(Status):
    def __init__(self, pv_template):
        super().__init__(pv_template)
        self.status = "successful"


@service
def test_pvs(pv_template: str) -> Status:
    try:
        df = get_dataframe()
        # Get the process variables for TEMPLATE
        process_variables = get_pvs(pv_template)

        pvs = []
        for a in process_variables:
            c = dict(a.items())
            pvs.append(c)

        if len(pvs) == 0:
            raise Exception(f'No PVS for template {pv_template}')

        s = pv_exec.compile_pvs(pv_template, pvs)
        if s is not None and isinstance(s, ErrorStatus):
            return s

        dataset = "survey"
        df.apply(pv_exec.modify_values, axis=1, dataset=dataset, pvs=pvs)
        return SuccessfulStatus(pv_template)

    except pv_exec.PVExecutionError as err:
        return ErrorStatus(pv_template, "PV execution failed: " + err.errorMessage, err.PV)


def get_pvs(template: str):
    engine = db.get_sql_connection()

    if engine is None:
        raise ConnectionError("Cannot get database connection")

    with engine.connect() as conn:
        sql = f"""
            SELECT PV_NAME as PROCVAR_NAME, PV_DEF as PROCVAR_RULE FROM PROCESS_VARIABLE_PY 
            WHERE RUN_ID = '{template}' order by PROCESS_VARIABLE_ID
        """
        v = conn.engine.execute(sql)
        return v.fetchall()


if __name__ == "__main__":
    template = 'TEMPLATE'

    status = test_pvs(template)
    r = test_pvs(template)
    if r is not None:
        if isinstance(r, SuccessfulStatus):
            result = {
                'status': 'successful',
                'template': template,
            }
            print(falcon.json.dumps(result))
        else:
            result = {
                'status': r.status,
                'template': r.template,
                'errorMessage': r.errorMessage,
                'PV': r.PV
            }
            print(falcon.json.dumps(result))
