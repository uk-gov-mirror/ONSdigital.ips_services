from ips_common.ips_logging import log
from ips.services.dataimport import CSVType
from ips.persistence.persistence import insert_from_dataframe, delete_from_table

NON_RESPONSE_TABLE = 'NON_RESPONSE_DATA'
insert_non_response = insert_from_dataframe(NON_RESPONSE_TABLE, "append")
delete_non_response = delete_from_table(NON_RESPONSE_TABLE)


def import_non_response(run_id, dataframe):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace('Non Response', CSVType.NonResponse.value, inplace=True)

    try:
        delete_non_response(run_id=run_id)
        insert_non_response(dataframe)

    except Exception as err:
        log.error(f"Cannot insert non_response dataframe into table: {err}")
        return None
