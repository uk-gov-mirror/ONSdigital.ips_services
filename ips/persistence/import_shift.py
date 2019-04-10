import io

import pandas as pd
from ips_common.ips_logging import log

from ips.persistence.persistence import insert_from_dataframe, delete_from_table
from ips.services.dataimport import CSVType

SHIFT_TABLE = 'SHIFT_DATA'
insert_shift = insert_from_dataframe(SHIFT_TABLE, "append")
delete_shift = delete_from_table(SHIFT_TABLE)


def import_shift_from_stream(run_id, data):
    df = pd.read_csv(io.BytesIO(data), encoding="ISO-8859-1", engine="python")
    log.debug("Importing shift data from stream")
    _import_shift_data(run_id, df)


def import_shift_from_file(run_id, shift_data_path):
    df = pd.read_csv(shift_data_path, encoding="ISO-8859-1", engine="python")
    log.debug(f"Importing shift data from file: {shift_data_path}")
    _import_shift_data(run_id, df)


def _import_shift_data(run_id, dataframe):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace([CSVType.Shift.name], CSVType.Shift.value, inplace=True)

    try:
        delete_shift(run_id=run_id)
        insert_shift(dataframe)
    except Exception as err:
        log.error(f"Cannot insert shift_data dataframe into database: {err}")
        return None
