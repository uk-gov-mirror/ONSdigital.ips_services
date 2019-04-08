import io

import pandas
import pandas as pd
from ips_common.logging import log

import ips.services.dataimport.schemas.traffic_schema as traffic_schema
from ips.persistence.persistence import execute_sql, insert_from_dataframe

TRAFFIC_TABLE = 'TRAFFIC_DATA'
insert_traffic = insert_from_dataframe(TRAFFIC_TABLE, "append")
delete_traffic = execute_sql()


def import_traffic_from_stream(import_type, run_id, data):
    df: pandas.DataFrame = pandas.read_csv(io.BytesIO(data), encoding="ISO-8859-1", engine="python")
    log.debug("Importing traffic data from stream")
    _import_traffic_data(import_type, df, run_id)


def import_traffic_from_file(import_type, run_id, traffic_data_path):
    data_schema = traffic_schema.get_schema()
    df: pandas.DataFrame = pd.read_csv(traffic_data_path, engine="python", dtype=data_schema)
    log.debug(f"Importing traffic data from file: {traffic_data_path}")
    _import_traffic_data(import_type, df, run_id)


def _import_traffic_data(import_type, dataframe, run_id):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    datasource_type = import_type.name
    datasource_id = import_type.value

    datasource_id = datasource_id
    dataframe['DATA_SOURCE_ID'].replace([datasource_type], datasource_id, inplace=True)

    sql = f"""
              DELETE FROM {TRAFFIC_TABLE}
              WHERE RUN_ID = '{run_id}'
              AND DATA_SOURCE_ID = '{datasource_id}'
    """

    try:
        delete_traffic(sql)
        insert_traffic(dataframe)
    except Exception as err:
        log.error(f"Cannot insert traffic_data dataframe into database: {err}")
        return None
