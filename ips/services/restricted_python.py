import ips.persistence.sql as db
from ips.util import process_variables as pv_exec
from ips.services.dataimport.import_survey import import_survey
from ips.services import service
import pandas as pd
import os


class InvalidPVs(Exception):
    pass


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


@service
def test_pvs(template: str) -> None:
    try:
        df = get_dataframe()
        # Get the process variables for TEMPLATE
        process_variables = get_pvs(template)

        pvs = []
        for a in process_variables:
            c = dict(a.items())
            pvs.append(c)

        if len(pvs) == 0:
            raise Exception(f'No PVS for template {template}')
        pv_exec.compile_pvs(pvs)
        dataset = "survey"
        df.apply(pv_exec.modify_values, axis=1, dataset=dataset, pvs=pvs)

    except Exception as err:
        raise InvalidPVs("Invalid PVs: " + str(err))


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
    test_pvs('TEMPLATE')
