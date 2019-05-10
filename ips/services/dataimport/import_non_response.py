import io

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_non_response as nr
from ips.services import service
from ips.services.dataimport.schemas import non_response_schema


@service
def import_nonresponse(run_id, data):
    log.info(f"Importing non_response data")
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=non_response_schema.get_schema()
    )

    _validate_data(df)
    nr.import_non_response(run_id, df)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame) -> bool:
    pass
