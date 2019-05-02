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

    df = pd.concat(pool.map(func, df_split), sort=True)

    pool.close()
    pool.join()

    return df


def log_warnings(warning_str: str) -> Callable[[pd.DataFrame], None]:
    def log_message(df: pd.DataFrame, items: int = 2):
        for index, record in df.iterrows():
            warn = f"{warning_str} "
            for i in range(items):
                warn += f"[{df.columns[i]} = {str(record[i])}]"
                if i != (items - 1):
                    warn += " : "
            log.warning(warn)

    return log_message


def log_errors(error_str: str) -> Callable[[pd.DataFrame], None]:
    def log_message(df: pd.DataFrame):
        for index, record in df.iterrows():
            log(f"{error_str} [{df.columns[0]} = {str(record[0])}]")

    return log_message
