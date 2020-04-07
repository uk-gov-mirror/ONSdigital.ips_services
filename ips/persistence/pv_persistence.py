import falcon
import pandas as pd
from ips.util.services_logging import log

from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_dataframe

PROCESS_VARIABLES_TABLE = 'PROCESS_VARIABLE_PY'

get_pv = read_table_values(PROCESS_VARIABLES_TABLE)
delete_pv = delete_from_table(PROCESS_VARIABLES_TABLE)
insert_pv = insert_from_dataframe(PROCESS_VARIABLES_TABLE, "append")
insert_single_pv = insert_from_dataframe(PROCESS_VARIABLES_TABLE)

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 10000)


def get_process_variables(run_id=None):
    data = get_pv()
    data['PV_DEF'] = data['PV_DEF'].str.replace('&lt;', '<').str.replace('&gt;', '>')

    if data.empty:
        error = f"PROCESS_VARIABLES table is empty."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    if run_id:
        data = data.loc[data['RUN_ID'] == run_id]

        if data.empty:
            error = f"Run id, {run_id}, is not in the PROCESS_VARIABLES table."
            log.error(error)
            raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    data.sort_values('PROCESS_VARIABLE_ID', inplace=True)
    data.index = range(0, len(data))
    output = data.to_json(orient='records')
    return output


def create_process_variables(data, run_id):
    df = pd.DataFrame(data)

    df.index = range(0, len(df))
    if df.empty:
        raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', 'The JSON was empty.')

    df['RUN_ID'] = run_id
    df['PV_DEF'] = df['PV_DEF'].str.replace('&lt;', '<').str.replace('&gt;', '>')
    insert_pv(df)


def edit_process_variable(data, run_id):
    pv_name = data['PV_NAME']
    pv_def = data['PV_DEF']
    df = get_pv()

    df['PV_DEF'] = df['PV_DEF'].str.replace('&lt;', '<').str.replace('&gt;', '>')

    if df.empty:
        error = f"PROCESS_VARIABLES table is empty."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    og_record = df.loc[(df['RUN_ID'] == run_id) & (df['PV_NAME'] == pv_name)]

    df.loc[(df['RUN_ID'] == run_id) & (df['PV_NAME'] == pv_name), "PV_DEF"] = pv_def
    record = df.loc[(df['RUN_ID'] == run_id) & (df['PV_NAME'] == pv_name)]

    pv_id = record['PROCESS_VARIABLE_ID']

    delete_pv(RUN_ID=run_id, PROCESS_VARIABLE_ID=int(pv_id))
    try:
        insert_pv(record)
        return True
    except:
        insert_pv(og_record)
        return False


def delete_process_variables(run_id):
    delete_pv(run_id=run_id)
