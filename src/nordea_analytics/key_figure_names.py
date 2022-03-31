from enum import Enum


class BondKeyFigureName(Enum):
    """Bond key figure names available in the service."""

    AccruedInterest = "accint"
    AssetSwapSpread = "asw"
    BPV = "bpvp"
    BPV10Y = "bpv10y"
    BPV15Y = "bpv15y"
    BPV1Y = "bpv1y"
    BPV20Y = "bpv20y"
    BPV25Y = "bpv25y"
    BPV2Y = "bpv2y"
    BPV30Y = "bpv30y"
    BPV3M = "bpv3m"
    BPV5Y = "bpv5y"
    BPV6M = "bpv6m"
    BPV7Y = "bpv7y"
    Coupon = "coupon"
    CVX = "cvxp"
    FWDuration_Deterministic = "fwduration"
    GovSpread = "gov_spread"
    HistoricalCapitalGain = "dd_princ"
    HistoricalReturn = "dd_total"
    HistoricalReturnAccumulated = "dd_total_acc"
    HorizonReturn12M = "return12m"
    HorizonReturn12M100 = "return12m100"
    HorizonReturn12M25 = "return12m25"
    HorizonReturn12M50 = "return12m50"
    HorizonReturn12M75 = "return12m75"
    HorizonReturn12Mm100 = "return12m-100"
    HorizonReturn12Mm25 = "return12m-25"
    HorizonReturn12Mm50 = "return12m-50"
    HorizonReturn12Mm75 = "return12m-75"
    HorizonReturn3M = "return3m"
    HorizonReturn3M50 = "return3m50"
    HorizonReturn3Mm50 = "return3m-50"
    HorizonReturn6M = "return6m"
    HorizonReturn6M50 = "return6m50"
    HorizonReturn6Mm50 = "return6m-50"
    HorizonReturnCapital12M = "capital12m"
    HorizonReturnCapital12M100 = "capital12m100"
    HorizonReturnCapital12M25 = "capital12m25"
    HorizonReturnCapital12M50 = "capital12m50"
    HorizonReturnCapital12M75 = "capital12m75"
    HorizonReturnCapital12Mm100 = "capital12m-100"
    HorizonReturnCapital12Mm25 = "capital12m-25"
    HorizonReturnCapital12Mm50 = "capital12m-50"
    HorizonReturnCapital12Mm75 = "capital12m-75"
    HorizonReturnCapital3M = "capital3m"
    HorizonReturnCapital3M50 = "capital3m50"
    HorizonReturnCapital3Mm50 = "capital3m-50"
    HorizonReturnCapital6M = "capital6m"
    HorizonReturnCapital6M50 = "capital6m50"
    HorizonReturnCapital6Mm50 = "capital6m-50"
    HorizonReturnInterest12M = "interest12m"
    HorizonReturnInterest12M100 = "interest12m100"
    HorizonReturnInterest12M25 = "interest12m25"
    HorizonReturnInterest12M50 = "interest12m50"
    HorizonReturnInterest12M75 = "interest12m75"
    HorizonReturnInterest12Mm100 = "interest12m-100"
    HorizonReturnInterest12Mm25 = "interest12m-25"
    HorizonReturnInterest12Mm50 = "interest12m-50"
    HorizonReturnInterest12Mm75 = "interest12m-75"
    HorizonReturnInterest3M = "interest3m"
    HorizonReturnInterest3M50 = "interest3m50"
    HorizonReturnInterest3Mm50 = "interest3m-50"
    HorizonReturnInterest6M = "interest6m"
    HorizonReturnInterest6M50 = "interest6m50"
    HorizonReturnInterest6Mm50 = "interest6m-50"
    MacauleyDuration_Deterministic = "macauley_duration"
    MaxOutstandingAmount = "max_outstanding_amount"
    ModifiedDuration_Deterministic = "modduration"
    OAModifiedDuration = "modified_bpv"
    OAS_3M = "libor_3m_spread"
    OAS_6M = "libor_spread"
    OAS_GOV = "govoas"
    OAS_OIS = "oas"
    OASRisk = "oasrisk"
    OATheta = "oatheta"
    OutstandingAmount = "outstanding_amount"
    OutstandingAmountCorrected = "corrected_outstanding_amount"
    PaymentScheduled = "schedpayment"
    PaymentTotal = "totpaymentamt"
    PrePayment = "prepublished_prepayment"
    PrePaymentPercentage = "prepayment"
    PrePaymentPreliminary = "prelimprepayment"
    PrePaymentPreliminaryPercentage = "ppp"
    PresentValue = "present_value"
    PriceClean = "clean_price"
    PriceDirty = "present_value"
    PriceKF = "official_price"
    Pricem100 = "pm100"
    Pricem200 = "pm200"
    Pricem300 = "pm300"
    Pricem400 = "pm400"
    Pricem50 = "pm50"
    Pricep100 = "pp100"
    Pricep200 = "pp200"
    Pricep300 = "pp300"
    Pricep400 = "pp400"
    Pricep50 = "pp50"
    PriceSpread = "price_spread"
    PriceTheoretical = "theoretical_price"
    Quote = "quote"
    QuotedSize = "quotedsize"
    Spread = "spread"
    SpreadRisk = "spread_risk"
    SwapSpread = "swap_spread"
    Theta = "theta"
    Vega = "vega"
    WAL_Deterministic = "detwal"
    YCS_3M = "libor_3m_spread"
    YCS_6M = "libor_spread"
    YCS_GOV = "govycs"
    YCS_OIS = "ycs"
    Yield = "yield"


class TimeSeriesKeyFigureName(Enum):
    """Time series key figure names available in the service."""

    AccruedInterest = "accint"
    AssetSwapSpread = "asw"
    BPV = "bpvp"
    BPV10Y = "bpv10y"
    BPV15Y = "bpv15y"
    BPV1Y = "bpv1y"
    BPV20Y = "bpv20y"
    BPV25Y = "bpv25y"
    BPV2Y = "bpv2y"
    BPV30Y = "bpv30y"
    BPV3M = "bpv3m"
    BPV5Y = "bpv5y"
    BPV6M = "bpv6m"
    BPV7Y = "bpv7y"
    Coupon = "coupon"
    CVX = "cvxp"
    FWDuration_Deterministic = "fwduration"
    GovSpread = "gov_spread"
    HistoricalCapitalGain = "dd_princ"
    HistoricalReturn = "dd_total"
    HistoricalReturnAccumulated = "dd_total_acc"
    HorizonReturn12M = "return12m"
    HorizonReturn12M100 = "return12m100"
    HorizonReturn12M25 = "return12m25"
    HorizonReturn12M50 = "return12m50"
    HorizonReturn12M75 = "return12m75"
    HorizonReturn12Mm100 = "return12m-100"
    HorizonReturn12Mm25 = "return12m-25"
    HorizonReturn12Mm50 = "return12m-50"
    HorizonReturn12Mm75 = "return12m-75"
    HorizonReturn3M = "return3m"
    HorizonReturn3M50 = "return3m50"
    HorizonReturn3Mm50 = "return3m-50"
    HorizonReturn6M = "return6m"
    HorizonReturn6M50 = "return6m50"
    HorizonReturn6Mm50 = "return6m-50"
    HorizonReturnCapital12M = "capital12m"
    HorizonReturnCapital12M100 = "capital12m100"
    HorizonReturnCapital12M25 = "capital12m25"
    HorizonReturnCapital12M50 = "capital12m50"
    HorizonReturnCapital12M75 = "capital12m75"
    HorizonReturnCapital12Mm100 = "capital12m-100"
    HorizonReturnCapital12Mm25 = "capital12m-25"
    HorizonReturnCapital12Mm50 = "capital12m-50"
    HorizonReturnCapital12Mm75 = "capital12m-75"
    HorizonReturnCapital3M = "capital3m"
    HorizonReturnCapital3M50 = "capital3m50"
    HorizonReturnCapital3Mm50 = "capital3m-50"
    HorizonReturnCapital6M = "capital6m"
    HorizonReturnCapital6M50 = "capital6m50"
    HorizonReturnCapital6Mm50 = "capital6m-50"
    HorizonReturnInterest12M = "interest12m"
    HorizonReturnInterest12M100 = "interest12m100"
    HorizonReturnInterest12M25 = "interest12m25"
    HorizonReturnInterest12M50 = "interest12m50"
    HorizonReturnInterest12M75 = "interest12m75"
    HorizonReturnInterest12Mm100 = "interest12m-100"
    HorizonReturnInterest12Mm25 = "interest12m-25"
    HorizonReturnInterest12Mm50 = "interest12m-50"
    HorizonReturnInterest12Mm75 = "interest12m-75"
    HorizonReturnInterest3M = "interest3m"
    HorizonReturnInterest3M50 = "interest3m50"
    HorizonReturnInterest3Mm50 = "interest3m-50"
    HorizonReturnInterest6M = "interest6m"
    HorizonReturnInterest6M50 = "interest6m50"
    HorizonReturnInterest6Mm50 = "interest6m-50"
    MacauleyDuration_Deterministic = "macauley_duration"
    MaxOutstandingAmount = "max_outstanding_amount"
    ModifiedDuration_Deterministic = "modduration"
    OAModifiedDuration = "modified_bpv"
    OAS_3M = "libor_3m_spread"
    OAS_6M = "libor_spread"
    OAS_GOV = "govoas"
    OAS_OIS = "oas"
    OASRisk = "oasrisk"
    OATheta = "oatheta"
    OutstandingAmount = "outstanding_amount"
    OutstandingAmountCorrected = "corrected_outstanding_amount"
    PaymentScheduled = "schedpayment"
    PaymentTotal = "totpaymentamt"
    PrePayment = "prepublished_prepayment"
    PrePaymentPercentage = "prepayment"
    PrePaymentPreliminary = "prelimprepayment"
    PrePaymentPreliminaryPercentage = "ppp"
    PresentValue = "present_value"
    PriceClean = "price"
    PriceDirty = "present_value"
    PriceKF = "official_price"
    Pricem100 = "pm100"
    Pricem200 = "pm200"
    Pricem300 = "pm300"
    Pricem400 = "pm400"
    Pricem50 = "pm50"
    Pricep100 = "pp100"
    Pricep200 = "pp200"
    Pricep300 = "pp300"
    Pricep400 = "pp400"
    Pricep50 = "pp50"
    PriceSpread = "price_spread"
    PriceTheoretical = "theoretical_price"
    Quote = "quote"
    QuotedSize = "quotedsize"
    Spread = "spread"
    SpreadRisk = "spread_risk"
    SwapSpread = "swap_spread"
    Theta = "theta"
    Vega = "vega"
    WAL_Deterministic = "detwal"
    YCS_3M = "libor_3m_spread"
    YCS_6M = "libor_spread"
    YCS_GOV = "govycs"
    YCS_OIS = "ycs"
    Yield = "yield"


class CalculatedBondKeyFigureName(Enum):
    """Bond key figure names that can be calculated in the service."""

    ASW_MM = "aswmm"
    AssetSwapSpread = "asw"
    ASW_PP = "aswpp"
    BPV = "bpv"
    BPV_Ladder = "bpvladder"
    CVX = "cvx"
    ExpectedCashflow = "expectedcashflow"
    MacaulayDuration = "macdur"
    ModifiedDuration = "moddur"
    PriceClean = "price"
    Spread = "spread"
    SpreadRisk = "spreadrisk"
    Yield = "yield"


class HorizonCalculatedBondKeyFigureName(Enum):
    """Bond key figure names that can be horizon calculated in the service."""

    BPV = "bpv"
    CVX = "cvx"
    PriceAtHorizon = "price_at_horizon"
    PriceClean = "price"
    ReturnInterest = "return_interest"
    ReturnInterestAmount = "return_interest_amount"
    ReturnPrincipal = "return_principal"
    ReturnPrincipalAmount = "return_principal_amount"
    Spread = "spread"


class LiveBondKeyFigureName(Enum):
    """Bond key figure names available live in the service."""

    CVX = "cvx"
    BPV = "bpv"
    GovSpread = "gov spread"
    OAS_3M = "libor 3m spread"
    OAS_6M = "libor 6m spread"
    Quote = "quote"
    Spread = "spread"
    SwapSpread = "swap spread"
    Yield = "yield"
