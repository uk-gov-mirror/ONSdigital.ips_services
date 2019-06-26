import math
import pandas as pd

# This needs to be in a separate file so that it can be accessed by the configuration module

# use SAS style rounding
from ips.util.services_configuration import ServicesConfiguration
from ips.util.services_logging import log


def _sas_round(n, decimals=0):
    if not isinstance(n, (float, int)):
        fare = pd.to_numeric(n, errors="coerce")
    else:
        fare = n

    if math.isnan(fare):
        return math.nan

    multiplier = 10 ** decimals

    return math.floor(n * multiplier + 0.5) / multiplier


# set to python default rounding unless overridden in configuration
ips_rounding = round

if ServicesConfiguration().sas_rounding():
    log.debug("Selecting SAS style rounding")
    ips_rounding = _sas_round
else:
    log.debug("Selecting Python style rounding")
