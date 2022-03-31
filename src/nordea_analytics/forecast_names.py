from enum import Enum


class YieldCountry(Enum):
    """Yield countries available in the service."""

    AU = "AU"
    BR = "BR"
    CA = "CA"
    CN = "CN"
    CZ = "CZ"
    DK = "DK"
    EU = "EU"
    HU = "HU"
    IN = "IN"
    JP = "JP"
    NO = "NO"
    PL = "PL"
    SE = "SE"
    CH = "CH"
    GB = "GB"
    US = "US"


class YieldType(Enum):
    """Yield types available in the service."""

    Govt = "govt"
    Libor = "libor"
    Leading = "leading"
    Swap = "swap"


class YieldHorizon(Enum):
    """Yield horizon available in the service."""

    Horizon_3M = "3M"
    Horizon_2Y = "2Y"
    Horizon_5Y = "5Y"
    Horizon_10Y = "10Y"
    Horizon_30Y = "30Y"
