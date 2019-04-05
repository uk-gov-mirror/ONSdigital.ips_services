import falcon

from ips_common.logging import log

from ips.persistence.runs_persistence import get_runs, create_run_data, delete_run, insert_run
from ips.services import service


@service
def get_run():

    data = get_runs()

    if data.empty:
        error = f"No run data"
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400, 'Data Error', error)

    try:
        return data.to_json(orient='records')
    except ValueError:
        error = f"Could not decode the request body. The JSON was invalid."
        log.error(error)
        raise falcon.HTTPError(falcon.HTTP_400,
                               'Invalid JSON',
                               'Could not decode the request body. The JSON was invalid.')


@service
def create_run(data):
    create_run_data(data)


@service
def edit_run(run_id, data):
    delete_run(run_id=run_id)
    insert_run(data)
