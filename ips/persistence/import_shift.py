from ips.util.services_logging import log
from ips.persistence.persistence import insert_from_dataframe, delete_from_table
from ips.services.dataimport import CSVType

SHIFT_TABLE = 'SHIFT_DATA'
insert_shift = insert_from_dataframe(SHIFT_TABLE, "append")
delete_shift = delete_from_table(SHIFT_TABLE)


def import_shift_data(run_id, dataframe):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace([CSVType.Shift.name], CSVType.Shift.value, inplace=True)

    try:
        delete_shift(run_id=run_id)
        log.info("Importing Shift data")
        insert_shift(dataframe)

    except Exception as err:
        log.error(f"Cannot insert Shift data into database: {err}")
        return None
