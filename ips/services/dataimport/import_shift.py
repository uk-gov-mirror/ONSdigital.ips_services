import io

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_shift as db
from ips.services import service
from ips.services.dataimport.schemas import shift_schema


@service
def import_shift(run_id, data):
    log.info("Importing shift data")
    df = pd.read_csv(
        io.BytesIO(data),
        encoding="ISO-8859-1",
        engine="python",
        dtype=shift_schema.get_schema()
    )

    _validate_data(df)
    db.import_shift_data(run_id, df)
    return df


# noinspection PyUnusedLocal
def _validate_data(data: pd.DataFrame) -> bool:
    pass
