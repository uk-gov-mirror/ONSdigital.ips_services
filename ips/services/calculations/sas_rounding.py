import math
import pandas as pd


# This needs to be in a separate file so that it can be accessed by the configuration module

# use SAS style rounding
def sas_round(n, decimals=0):
    if not isinstance(n, (float, int)):
        fare = pd.to_numeric(n, errors="coerce")
    else:
        fare = n

    if math.isnan(fare):
        return math.nan

    multiplier = 10 ** decimals

    return math.floor(n * multiplier + 0.5) / multiplier
