from ips.util.services_logging import log

from ips.persistence.persistence import insert_from_dataframe, delete_from_table
from ips.services.dataimport import CSVType

UNSAMPLED_OOH_DATA = 'UNSAMPLED_OOH_DATA'
insert_unsampled = insert_from_dataframe(UNSAMPLED_OOH_DATA, "append")
delete_unsampled = delete_from_table(UNSAMPLED_OOH_DATA)


def import_unsampled(run_id, dataframe):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    # replace "REGION" values with 0 if not an expected value
    dataframe['REGION'].replace(['None', "", ".", 'nan'], 0, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace([CSVType.Unsampled.name], CSVType.Unsampled.value, inplace=True)

    try:
        delete_unsampled(run_id=run_id)
        log.info("Importing Unsampled data")
        insert_unsampled(dataframe)
    except Exception as err:
        log.error(f"Cannot insert Unsampled_data into database: {err}")
        return None
