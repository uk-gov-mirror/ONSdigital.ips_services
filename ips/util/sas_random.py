from mpmath import *
import pandas
from ips.util.services_logging import log

mp.dps = 17


def seed(inputseed):
    global seedlist
    seedlist = [inputseed]


def sas_random():
    m = mp.mpf(2 ** 31 - 1)
    a = mp.mpf(397204094)
    inputseed = seedlist[-1]

    if inputseed < 1:
        inputseed = inputseed * m

    outputseed = inputseed * a % m

    seedlist.append(outputseed)

    return outputseed / m
