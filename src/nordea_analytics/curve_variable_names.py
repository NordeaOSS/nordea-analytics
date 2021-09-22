from enum import Enum


class CurveType(Enum):
    """Used to control which curve types are available in the service."""

    Bootstrap = "bootstrap"
    NelsonSiegel = "nelsonsiegel"
    XNelsonSiegel = "xnelsonsiegel"
    HybridXNS = "hybridxns"
    HybridNS = "hybridns"
    YTMCurve = "ytm curve"
    ParCurve = "par curve"
    DurationCurve = "duration curve"


class TimeConvention(Enum):
    """Used to control which time conventions are available in the service."""

    TC_30360 = "30360"
    TC_30E360 = "30e360"
    TC_30EP360 = "30ep360"
    Act360 = "act360"
    Act365 = "act365"
    ISDAAct = "sidaact"
    ActNL365 = "actnl365"
    AFB = "afb"
    ActNL360 = "actnl360"


class SpotForward(Enum):
    """Used to control which spot/forward are available in the service."""

    Spot = "Spot"
    Forward = "Forward"
    ImpliedForward = "ImpliedForward"
