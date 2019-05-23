from enum import Enum


class CSVType(Enum):
    Sea = 1
    Air = 2
    Tunnel = 3
    Shift = 4
    NonResponse = 5
    Unsampled = 6
    Survey = 7
