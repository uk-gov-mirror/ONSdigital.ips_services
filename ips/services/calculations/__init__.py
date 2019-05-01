import multiprocessing
from typing import Callable

import numpy as np
import pandas as pd
from ips_common.ips_logging import log

THRESHOLD_CAP = 4000


def parallelise_dataframe(df, func):
    num_partitions = multiprocessing.cpu_count()
    df_split = np.array_split(df, num_partitions)
    pool = multiprocessing.Pool(num_partitions)

    df = pd.concat(pool.map(func, df_split))

    pool.close()
    pool.join()

    return df


def log_warnings(warning_str: str) -> Callable[[pd.DataFrame], None]:

    def log_message(df: pd.DataFrame):
        for index, record in df.iterrows():
            warn = f"{warning_str} {df.columns[0]} = {str(record[0])} {df.columns[1]} = {str(record[1])}"
            log.warning(warn)

    return log_message

