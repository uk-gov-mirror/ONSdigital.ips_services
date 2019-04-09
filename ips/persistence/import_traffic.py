import io

import pandas
import pandas as pd
from ips_common.ips_logging import log

import ips.services.dataimport.schemas.traffic_schema as traffic_schema
from ips.persistence.persistence import insert_from_dataframe, delete_from_table

TRAFFIC_TABLE = 'TRAFFIC_DATA'
insert_traffic = insert_from_dataframe(TRAFFIC_TABLE, "append")
delete_traffic = delete_from_table(TRAFFIC_TABLE)


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

    dataframe['DATA_SOURCE_ID'].replace([import_type.name], import_type.value, inplace=True)

    try:
        delete_traffic(run_id=run_id)
        insert_traffic(dataframe)
    except Exception as err:
        log.error(f"Cannot insert traffic_data dataframe into database: {err}")
        return None
