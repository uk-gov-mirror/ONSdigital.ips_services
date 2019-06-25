from ips.services.calculations.sas_rounding import sas_round
import math


def test_rounding():
    assert sas_round(5.62354235523, 2) == 5.62

    assert sas_round(5.62354235523, 0) == 6
    assert sas_round(5.123, 2) == 5.12

    assert math.isnan(sas_round(math.nan)) == True
