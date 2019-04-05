import falcon
import pandas
from ips_common.logging import log

from ips.persistence.pv_persistence import get_pv, insert_pv, delete_pv
from ips.services import service


@service
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


@service
def create_process_variables(data, run_id):
    df = pandas.DataFrame(data)

    df.index = range(0, len(df))

    if df.empty:
        raise falcon.HTTPError(falcon.HTTP_400, 'Invalid request', 'The JSON was empty.')

    df['RUN_ID'] = run_id

    df['PV_DEF'] = df['PV_DEF'].str.replace('&lt;', '<').str.replace('&gt;', '>')

    insert_pv(df)

    return falcon.HTTP_201


@service
def delete_process_variables(run_id):
    delete_pv(run_id=run_id)
