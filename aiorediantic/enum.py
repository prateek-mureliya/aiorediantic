from enum import Enum


class ExpireEnum(str, Enum):
    NX = "NX"
    XX = "XX"
    GT = "GT"
    LT = "LT"
