import falcon
from ips.util.services_logging import log
import pandas as pd
from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_dataframe, get_responses
import json

RUN_STEPS_TABLE = 'RUN_STEPS'
RESPONSE_TABLE = 'RESPONSE'

get_steps = read_table_values(RUN_STEPS_TABLE)
delete_steps = delete_from_table(RUN_STEPS_TABLE)
create_steps = insert_from_dataframe(RUN_STEPS_TABLE, "append")


def get_run_steps(run_id: str = None) -> str:
    data = get_steps()

    if data.empty:
        error = f"RUN_STEPS table is empty."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    if run_id:
        data = data.loc[data['RUN_ID'] == run_id]
        if data.empty:
            error = f"Run id, {run_id}, is not in the RUN_STEPS table."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    try:
        data_json = json.loads(data.to_json(orient='records'))
        for index,step in enumerate(data_json):
            responses = json.loads(get_responses(data_json[index]['STEP_NUMBER'], run_id))
            if len(responses) > 0:
                data_json[index]['Responses'] = responses
        return json.dumps(data_json)

    except ValueError:
        error = f"Could not decode the request body. The JSON was invalid."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400,
                               'Invalid JSON',
                               'Could not decode the request body. The JSON was invalid.')


def create_run_steps(run_id: str) -> None:
    calculation_steps = {
        1: 'Calculate Shift Weight',
        2: 'Calculate Non-Response Weight',
        3: 'Calculate Minimums Weight',
        4: 'Calculate Traffic Weight',
        5: 'Calculate Unsampled Weight',
        6: 'Calculate Imbalance Weight',
        7: 'Calculate Final Weight',
        8: 'Stay Imputation',
        9: 'Fares Imputation',
        10: 'Spend Imputation',
        11: 'Rail Imputation',
        12: 'Regional Weight',
        13: 'Town Stay and Expenditure Imputation',
        14: 'Air Miles'
    }

    data = []

    for key, value in calculation_steps.items():
        data.append({
            'RUN_ID': run_id,
            'STEP_NUMBER': key,
            'STEP_NAME': value,
            'STEP_STATUS': 0
        })

    delete_steps(run_id=run_id)
    df = pd.DataFrame(data)
    create_steps(df)


def edit_run_steps(run_id: str, value: str, step_number: str = None) -> None:
    df = get_steps()

    df = df.loc[df['RUN_ID'] == run_id]

    if step_number:
        df.loc[df['STEP_NUMBER'] == float(step_number), 'STEP_STATUS'] = float(value)
    else:
        df.STEP_STATUS = value

    df.index = range(0, len(df))

    delete_steps(run_id=run_id)
    create_steps(df)
