import pandas as pd
import numpy as np


# This needs to be in a separate file so that it can be accessed by the configuration module

# use SAS style rounding - truncates value to 'decimal' decimal places
# if decimals is a zero then it simply converts to an int
def sas_round(n, decimals=0):

    if not isinstance(n, (float, int)):
        fare = pd.to_numeric(n, errors="coerce")
    else:
        fare = n

    if fare == np.nan:
        return n
    multiplier = 10 ** decimals
    return (n * multiplier) / multiplier
