import falcon
from ips_common.ips_logging import log

from ips.persistence.persistence import read_table_values, delete_from_table, insert_from_json

RUNS_TABLE = 'RUN'

get_runs = read_table_values(RUNS_TABLE)
delete_run = delete_from_table(RUNS_TABLE)
create_run_data = insert_from_json(RUNS_TABLE, "append")
insert_run = insert_from_json(RUNS_TABLE, "append")


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


def create_run(data):
    create_run_data(data)


def edit_run(run_id, data):
    delete_run(run_id=run_id)
    insert_run(data)
