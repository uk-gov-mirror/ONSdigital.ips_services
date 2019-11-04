from datetime import datetime

import pandas as pd
import numpy as np
from ips.util.services_logging import log

from ips.persistence.persistence import select_data, insert_from_dataframe

EXPORT_DATA_DOWNLOAD = 'EXPORT_DATA_DOWNLOAD'

insert_downloads = insert_from_dataframe(EXPORT_DATA_DOWNLOAD, "append")


def export_from_table(run_id, table):
    df = select_data(column_name="*", table=table, condition1="RUN_ID", condition2=run_id)
    df.replace(to_replace=-1, value=np.nan, inplace=True)
    if table == "SURVEY_SUBSAMPLE":
        df = df.drop(columns=['RUN_ID'])
    csv = df.to_csv(index=False)
    return create_export_data_download(run_id, table, csv)


def create_export_data_download(run_id, table, csv):
    json_data = {
        'DATE_CREATED': [datetime.now().strftime('%Y-%d-%m %H:%M:%S')],
        'DOWNLOADABLE_DATA': [csv],
        'FILENAME': [table],
        'RUN_ID': [run_id],
        'SOURCE_TABLE': [table]
    }

    df = pd.DataFrame(json_data)
    insert_downloads(df)
    return json_data
