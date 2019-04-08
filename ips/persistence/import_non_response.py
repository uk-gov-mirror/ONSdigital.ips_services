import io

import pandas as pd
from ips_common.logging import log

from ips.services.dataimport.schemas import non_response_schema
from ips.persistence.persistence import execute_sql, insert_from_dataframe


NON_RESPONSE_TABLE = 'NON_RESPONSE_DATA'
insert_non_response = insert_from_dataframe(NON_RESPONSE_TABLE, "append")
delete_non_response = execute_sql()


def import_nonresponse_from_stream(run_id, data):
    df = pd.read_csv(io.BytesIO(data), encoding="ISO-8859-1", engine="python")
    log.debug("Importing non_response data from stream")
    _import_non_response(df, run_id)


def import_nonresponse_from_file(run_id, data_path):
    data_schema = non_response_schema.get_schema()
    df = pd.read_csv(data_path, encoding="ISO-8859-1", engine="python", dtype=data_schema)
    log.debug(f"Importing survey data from file {data_path}")
    return _import_non_response(df, run_id)


def _import_non_response(dataframe, run_id):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    datasource_id = 'NonResponse'

    datasource_id = datasource_id
    dataframe['DATA_SOURCE_ID'].replace(['Non Response'], datasource_id, inplace=True)

    sql = f"""
            DELETE FROM NON_RESPONSE_DATA
            WHERE RUN_ID = '{run_id}'
            """

    try:
        delete_non_response(sql)
        insert_non_response(dataframe)
    except Exception as err:
        log.error(f"Cannot insert non_response dataframe into table: {err}")
        return None
