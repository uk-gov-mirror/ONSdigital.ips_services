import io

import pandas as pd
from ips_common.ips_logging import log

import ips.persistence.import_non_response as nr
from ips.services import service
from ips.services.dataimport.schemas import non_response_schema


@service
def import_nonresponse_stream(run_id, data):
    log.debug(f"Importing non_response data from stream")
    return _import_non_response(run_id, io.BytesIO(data))


@service
def import_nonresponse_file(run_id, nr_data_path):
    log.debug(f"Importing non_response data from file {nr_data_path}")
    return _import_non_response(run_id, nr_data_path)


def _import_non_response(run_id, source):
    df = pd.read_csv(
        source,
        encoding="ISO-8859-1",
        engine="python",
        dtype=non_response_schema.get_schema()
    )

    _validate_data(df)
    nr.import_non_response(run_id, df)
    return df


def _validate_data(data: pd.DataFrame) -> bool:
    pass
