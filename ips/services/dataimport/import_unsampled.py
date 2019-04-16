import io

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_unsampled as db
from ips.services import service
from ips.services.dataimport.schemas import unsampled_schema


@service
def import_unsampled_stream(run_id, data):
    log.debug("Importing unsampled data from stream")
    return _import_unsampled(run_id, io.BytesIO(data))


@service
def import_unsampled_file(run_id, data):
    log.debug(f"Importing unsampled data from file: {data}")
    return _import_unsampled(run_id, data)


def _import_unsampled(run_id, source):
    df = pd.read_csv(
        source,
        encoding="ISO-8859-1",
        engine="python",
        dtype=unsampled_schema.get_schema()
    )

    _validate_data(df)
    db.import_unsampled(run_id, df)
    return df


def _validate_data(data: pd.DataFrame) -> bool:
    pass
