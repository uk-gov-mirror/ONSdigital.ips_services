import io

import pandas as pd
from ips_common.ips_logging import log

from ips.persistence.persistence import insert_from_dataframe, delete_from_table
from ips.services.dataimport import CSVType
from ips.services.dataimport.schemas import unsampled_schema

UNSAMPLED_OOH_DATA = 'UNSAMPLED_OOH_DATA'
insert_unsampled = insert_from_dataframe(UNSAMPLED_OOH_DATA, "append")
delete_unsampled = delete_from_table(UNSAMPLED_OOH_DATA)


def import_unsampled_from_stream(run_id, data):
    df = pd.read_csv(io.BytesIO(data), encoding="ISO-8859-1", engine="python")
    log.debug("Importing unsampled data from stream")
    _import_unsampled(run_id, df)


def import_unsampled_from_file(run_id, file_name):
    data_schema = unsampled_schema.get_schema()
    df = pd.read_csv(file_name, encoding="ISO-8859-1", engine="python", dtype=data_schema)
    log.debug(f"Importing unsampled data from file: {file_name}")
    _import_unsampled(run_id, df)


def _import_unsampled(run_id, dataframe):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    # replace "REGION" values with 0 if not an expected value
    dataframe['REGION'].replace(['None', "", ".", 'nan'], 0, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace([CSVType.Unsampled.name], CSVType.Unsampled.value, inplace=True)

    try:
        delete_unsampled(run_id=run_id)
        insert_unsampled(dataframe)
    except Exception as err:
        log.error(f"Cannot insert unsampled_data dataframe into database: {err}")
        return None
