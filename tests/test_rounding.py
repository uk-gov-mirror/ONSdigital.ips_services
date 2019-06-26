from ips.services.calculations.sas_rounding import ips_rounding
import math


def test_rounding():
    assert ips_rounding(5.62354235523, 2) == 5.62

    assert ips_rounding(5.62354235523, 0) == 6
    assert ips_rounding(5.123, 2) == 5.12

    assert math.isnan(ips_rounding(math.nan))
