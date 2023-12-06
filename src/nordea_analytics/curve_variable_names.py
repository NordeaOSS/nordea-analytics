from enum import Enum


class CurveName(Enum):
    """Most common available curves. Availability not limit to this list."""

    ATSGOV = "ATSGOV"
    BEFGOV = "BEFGOV"
    CHFGOV = "CHFGOV"
    CHFSWAP_Disc_OIS = "CHFSWAP DISC OIS"
    CHFSWAP_Fix_1D_OIS = "CHFSWAP FIX 1D OIS"
    CHFSWAP_Fix_1Y_OIS = "CHFSWAP FIX 1Y OIS"
    CHFSWAP_Fix_3M_OIS = "CHFSWAP FIX 3M OIS"
    CHFSWAP_Fix_6M_OIS = "CHFSWAP FIX 6M OIS"
    CHFSWAP_Libor = "CHFSWAP LIBOR"
    DEMGOV = "DEMGOV"
    DKKGOV = "DKKGOV"
    DKKMTGNYK = "DKKMTGNYKSOFTBLT"
    DKKSWAP = "DKKSWAP"
    DKKSWAP_Disc_OIS = "DKKSWAP DISC OIS"
    DKKSWAP_Disc_DESTR = "DKKSWAP DISC DESTR"
    DKKSWAP_Fix_1D_OIS = "DKKSWAP FIX 1D OIS"
    DKKSWAP_Fix_1D_DESTR = "DKKSWAP FIX 1D DESTR"
    DKKSWAP_Fix_1M_OIS = "DKKSWAP FIX 1M OIS"
    DKKSWAP_Fix_1Y_OIS = "DKKSWAP FIX 1Y OIS"
    DKKSWAP_Fix_3M_OIS = "DKKSWAP FIX 3M OIS"
    DKKSWAP_Fix_6M_OIS = "DKKSWAP FIX 6M OIS"
    DKKSWAP_Libor = "DKKSWAP LIBOR"
    DKKSWAP_Libor_3M = "DKKSWAP LIBOR 3M"
    ESPGOV = "ESPGOV"
    EURGOV = "EURGOV"
    EURSWAP = "EURSWAP"
    EURSWAP_Disc_ESTR = "EURSWAP DISC ESTR"
    EURSWAP_Disc_OIS = "EURSWAP DISC OIS"
    EURSWAP_Fix_1D_ESTR = "EURSWAP FIX 1D ESTR"
    EURSWAP_Fix_1D_OIS = "EURSWAP FIX 1D OIS"
    EURSWAP_Fix_1M_ESTR = "EURSWAP FIX 1M ESTR"
    EURSWAP_Fix_1Y_ESTR = "EURSWAP FIX 1Y ESTR"
    EURSWAP_Fix_3M_ESTR = "EURSWAP FIX 3M ESTR"
    EURSWAP_Fix_6M_ESTR = "EURSWAP FIX 6M ESTR"
    EURSWAP_Libor = "EURSWAP LIBOR"
    FIMGOV = "FIMGOV"
    FRFGOV = "FRFGOV"
    GBPGOV = "GBPGOV"
    GBPSWAP = "GBPSWAP"
    GBPSWAP_Disc_OIS = "GBPSWAP DISC OIS"
    GBPSWAP_Fix_1D_OIS = "GBPSWAP FIX 1D OIS"
    GBPSWAP_Fix_3M_OIS = "GBPSWAP FIX 3M OIS"
    GBPSWAP_Fix_6M_OIS = "GBPSWAP FIX 6M OIS"
    GBPSWAP_Libor = "GBPSWAP LIBOR"
    ITLGOV = "ITLGOV"
    JPYGOV = "JPYGOV"
    JPYSWAP = "JPYSWAP"
    JPYSWAP_Disc_OIS = "JPYSWAP DISC OIS"
    JPYSWAP_Fix_1D_OIS = "JPYSWAP FIX 1D OIS"
    JPYSWAP_Fix_3M_OIS = "JPYSWAP FIX 3M OIS"
    JPYSWAP_Fix_6M_OIS = "JPYSWAP FIX 6M OIS"
    JPYSWAP_Libor = "JPYSWAP LIBOR"
    NLGGOV = "NLGGOV"
    NOKGOV = "NOKGOV"
    NOKSWAP = "NOKSWAP"
    NOKSWAP_Disc_OIS = "NOKSWAP DISC OIS"
    NOKSWAP_Fix_1D_OIS = "NOKSWAP FIX 1D OIS"
    NOKSWAP_Fix_1Y_OIS = "NOKSWAP FIX 1Y OIS"
    NOKSWAP_Fix_3M_OIS = "NOKSWAP FIX 3M OIS"
    NOKSWAP_Fix_6M_OIS = "NOKSWAP FIX 6M OIS"
    NOKSWAP_Libor = "NOKSWAP LIBOR"
    PLNGOV = "PLNGOV"
    RUBSWAP = "RUBSWAP"
    RUBSWAP_Disc_OIS = "RUBSWAP DISC OIS"
    RUBSWAP_Fix_1D_OIS = "RUBSWAP FIX 1D OIS"
    RUBSWAP_Fix_3M_OIS = "RUBSWAP FIX 3M OIS"
    RUBSWAP_Fix_6M_OIS = "RUBSWAP FIX 6M OIS"
    RUBSWAP_Libor = "RUBSWAP LIBOR"
    SEKGOV = "SEKGOV"
    SEKMTGBLEND = "SEKMTGBLEND"
    SEKMTGNBH = "SEKMTGNBH"
    SEKMTGSEB = "SEKMTGSEB"
    SEKMTGSHYP = "SEKMTGSHYP"
    SEKMTGSWED = "SEKMTGSWED"
    SEKSWAP = "SEKSWAP"
    SEKSWAP_Disc_OIS = "SEKSWAP DISC OIS"
    SEKSWAP_Fix_1D_OIS = "SEKSWAP FIX 1D OIS"
    SEKSWAP_Fix_1Y_OIS = "SEKSWAP FIX 1Y OIS"
    SEKSWAP_Fix_3M_OIS = "SEKSWAP FIX 3M OIS"
    SEKSWAP_Fix_6M_OIS = "SEKSWAP FIX 6M OIS"
    SEKSWAP_Libor = "SEKSWAP LIBOR"
    USDGOV = "USDGOV"
    USDSWAP = "USDSWAP"
    USDSWAP_Disc_OIS = "USDSWAP DISC OIS"
    USDSWAP_Fix_1D_OIS = "USDSWAP FIX 1D OIS"
    USDSWAP_Fix_3M_OIS = "USDSWAP FIX 3M OIS"
    USDSWAP_Fix_6M_OIS = "USDSWAP FIX 6M OIS"
    USDSWAP_Libor = "USDSWAP LIBOR"


class CurveDefinitionName(Enum):
    """Most common available curves definition. Availability not limit to this list."""

    # Note that curve definition is limited to non-Infinity curves for externals.
    ATSGOV = "ATSGOV"
    BEFGOV = "BEFGOV"
    CHFGOV = "CHFGOV"
    DEMGOV = "DEMGOV"
    DKKGOV = "DKKGOV"
    DKKMTGNYK = "DKKMTGNYKSOFTBLT"
    DKKSWAP = "DKKSWAP"
    ESPGOV = "ESPGOV"
    EURGOV = "EURGOV"
    EURSWAP = "EURSWAP"
    FIMGOV = "FIMGOV"
    FRFGOV = "FRFGOV"
    GBPGOV = "GBPGOV"
    GBPSWAP = "GBPSWAP"
    ITLGOV = "ITLGOV"
    JPYGOV = "JPYGOV"
    JPYSWAP = "JPYSWAP"
    NLGGOV = "NLGGOV"
    NOKGOV = "NOKGOV"
    NOKSWAP = "NOKSWAP"
    NOKSWAP_Libor = "NOKSWAP LIBOR"
    PLNGOV = "PLNGOV"
    RUBSWAP = "RUBSWAP"
    SEKGOV = "SEKGOV"
    SEKMTGBLEND = "SEKMTGBLEND"
    SEKMTGNBH = "SEKMTGNBH"
    SEKMTGSEB = "SEKMTGSEB"
    SEKMTGSHYP = "SEKMTGSHYP"
    SEKMTGSWED = "SEKMTGSWED"
    SEKSWAP = "SEKSWAP"
    USDGOV = "USDGOV"
    USDSWAP = "USDSWAP"


class CurveType(Enum):
    """Curve types available in the service."""

    Bootstrap = "bootstrap"
    NelsonSiegel = "nelsonsiegel"
    XNelsonSiegel = "xnelsonsiegel"
    HybridXNS = "hybridxns"
    HybridNS = "hybridns"
    YTMCurve = "ytm curve"
    ParCurve = "par curve"
    DurationCurve = "duration curve"


class SpotForward(Enum):
    """Spot/forward available in the service."""

    Spot = "Spot"
    Forward = "Forward"
    ImpliedForward = "ImpliedForward"
