from ips_common.ips_logging import log

from ips.persistence.persistence import insert_from_dataframe, delete_from_table

TRAFFIC_TABLE = 'TRAFFIC_DATA'
insert_traffic = insert_from_dataframe(TRAFFIC_TABLE, "append")
delete_traffic = delete_from_table(TRAFFIC_TABLE)


def import_traffic_data(import_type, dataframe, run_id):
    dataframe.columns = dataframe.columns.str.upper()
    dataframe.columns = dataframe.columns.str.replace(' ', '')
    dataframe["RUN_ID"] = run_id
    dataframe.rename(columns={"DATASOURCE": "DATA_SOURCE_ID"}, inplace=True)

    dataframe['DATA_SOURCE_ID'].replace([import_type.name], import_type.value, inplace=True)

    try:
        delete_traffic(run_id=run_id, data_source_id=import_type.value)
        insert_traffic(dataframe)
    except Exception as err:
        log.error(f"Cannot insert traffic_data dataframe into database: {err}")
        return None
