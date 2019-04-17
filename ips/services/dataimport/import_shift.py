import io

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_shift as db
from ips.services import service
from ips.services.dataimport.schemas import shift_schema


@service
def import_shift_stream(run_id, data):
    log.info("Importing shift data from stream")
    return _import_shift(run_id, io.BytesIO(data))


@service
def import_shift_file(run_id, data_path):
    log.info(f"Importing shift data from file: {data_path}")
    return _import_shift(run_id, data_path)


def _import_shift(run_id, shift_data_path):
    df = pd.read_csv(
        shift_data_path,
        encoding="ISO-8859-1",
        engine="python",
        dtype=shift_schema.get_schema()
    )

    _validate_data(df)
    db.import_shift_data(run_id, df)
    return df


def _validate_data(data: pd.DataFrame) -> bool:
    pass
