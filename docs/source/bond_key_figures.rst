Bond Key Figures
=================
This section details the main key figures; what they mean and how they are calculated.
There are fundamentally two different kinds of key figures; stochastic and deterministic ones. The stochastic key figures
are those that require Monte Carlo simulation of interest rate paths and resulting prepayment behavior. The cash flow used is
resulting from prepayments as predicted by the prepayment model. The deterministic ones are those that can be computed
directly from the rate curves and use cash flow with no prepayments.

Danish Mortgage bonds with embedded optionality (Callable and Fixed Floaters) are modelled with the prepayment model,
and thus have option adjusted(OA) key figures. In these cases the prepayment model cash flow and the option
adjusted spread(OAS, described below) is used for the estimation. This is embedded where relevant, and thus
does not need to be specified when retrieving the key figure values.

Note, all bond key figures available in the library are listed in the :class:`BondKeyFigureName <nordea_analytics.key_figure_names.BondKeyFigureName>`
class and can be retrieved with the function :meth:`get_bond_key_figures() <nordea_analytics.nordea_analytics_service.core.NordeaAnalyticsCoreService.get_bond_key_figures>`.

Present Value (PV)
--------------------
**BondKeyFigureName.PresentValue**

The present value of a bond, with a spread of z.

.. math::
    PV(R, \sigma, z) = E^{Q}_{0}\left[\sum_{i} CF_{i}(R, \sigma) df_{i}(R, z)\right],

where:

:math:`E^{Q}_{0}` = Expected risk netural values given known information today information

:math:`R` = sets of rate curves (both discounting and forward)

:math:`\sigma` = represents the swaption volatility in the stochastic rate curve model

:math:`z` = the option adjusted spread

:math:`CF_{i}` = cash flow at time i

:math:`df_{i}` = discount factor at time i

Vega
------
**BondKeyFigureName.Vega**

This approximation of vega shows how much the present value changes with respect to the swaption volatility.

.. math::
    \nu = \frac{\delta}{\delta \sigma_{ATM}} PV

where :math:`\sigma_{ATM}` is the at-the-money swaption volatility.

BPV (Basis Point Value)
------------------------
**BondKeyFigureName.BPV**

This key figure shows how much the present value of a bond changes with respect to rates.

.. math::
    BPV = - \frac{\delta}{\delta R} PV(R)

CVX (Convexity)
------------------
**BondKeyFigureName.CVX**

This key figure shows how much the BPV of a bond changes if the rates decrease and increased by :math:`\Delta`

.. math::
    CVX = \frac{\frac{\delta}{\delta R} PV(R+\Delta) - \frac{\delta}{\delta R} PV(R)}{\Delta}

where :math:`\Delta` is 30bp for Danish Mortgage Bonds and 20bp for Capped floaters.

Modified duration
------------------
**BondKeyFigureName.OAModifiedDuration**

This key figure shows how much the present value of a bond changes with respect to changes in rates, per invested currency unit.

.. math::
    ModDur = \frac{BPV}{\frac{price_{dirty}}{par}}

where :math:`par` is the bond's notional (in most cases 100 DKK) and :math:`price_{dirty}` is bonds quote + accrued interests.

Theta
------
**BondKeyFigureName.Theta**

This key figure measures the drift of the bond price due to the passage of time.

.. math::
    \Theta = (r_{0} + spread) price_{dirty}

Horizon Return
---------------
**BondKeyFigureName.HorizonReturn3M**

**BondKeyFigureName.HorizonReturn6M**

**BondKeyFigureName.HorizonReturn12M**

Forward looking return of holding the bonds for x months while holding all other assumptions fixed.

Historical Return
------------------

**BondKeyFigureName.HistoricalReturnAccumulated**

The accumulated one day return of holding the bond. Starting AccReturn at t=0 is 100%.

.. math::
    AccReturn_{t(i)} = Return_{t(i-1)} + Retrun_{t(i)}


Accrued Interest
-----------------
**BondKeyFigureName.AccruedInterest**

This key figure shows how much interest a bond has accrued since the last coupon payment.

.. math::
    AI=c_{term}{t_{i}/t_{p}}

where :math:`c_{term}` is the coupon for the term in question(e.g.3%=4), :math:`t_{i}` the time in years since last
coupon payment and :math:`t_{p}` the time in years between the last payment and the next.

OA Spreads
-----------
**BondKeyFigureName.OAS_OIS**

Difference between the theoretical price and market price, expressed in terms of a spread to the interest rate curve.
The spread of the bond is solved in the following equation:

.. math::
    PV (OAS) = price_{dirty}

Below are listed other OA spread key figures, which are computed as described above using the relevant discount factor
in the PV function.

**BondKeyFigureName.OAS_GOV**

**BondKeyFigureName.OAS_3M**

**BondKeyFigureName.OAS_6M**

Yield Curve Spread (YCS)
-------------------------
**BondKeyFigureName.YCS_OIS**

**BondKeyFigureName.YCS_GOV**

**BondKeyFigureName.YCS_3M**

**BondKeyFigureName.YCS_6M**

Yield curve spreads(YCS) are estimated without taking the prepayment model into account, thus it uses the deterministic
PV for estimation:

.. math::
    PV(z)_{det} = \sum_{i} CF_{i}^{PP=0} e^{-(r_{i} + z)t_{i}}

As with OAS, the YCS is then estimated as:

.. math::
    PV (YCS) = price_{dirty}


Asset Swap Spread
------------------------
**BondKeyFigureName.AssetSwapSpread**

The spread is the pick-up you obtain from swapping the fixed leg into a floating yield compared
to an interbank offered rate. The prepayments are calculated as optimal prepayment behaviour. Asset swap spread is only
calculated when the price of the bond is below 100.

Payments
----------
Prepayment
^^^^^^^^^^^
**BondKeyFigureName.PrePayment**

Prepayments are extra ordinary payments that happen when a borrower decides to exercise the
prepayment optionality embedded in the Danish Mortgage bond. Prepayments are payed out on settlement date with other
scheduled payments.

The bond key figure name **BondKeyFigureName.PrepaymentPercentage** represents the pre-published payment amount as a
percentage of outstanding amount;

Preliminary Prepayment
^^^^^^^^^^^^^^^^^^^^^^^
**BondKeyFigureName.PreliminaryPrepayment**

The prepayment amount known for the upcoming settlement date. Published weekly, most often on Mondays.

The key figure name **BondKeyFigureName.PreliminaryPrepaymentPercentage** represents the preliminary pre payment
amount as a percentage of outstanding amount.

Payment Scheduled
^^^^^^^^^^^^^^^^^^^
**BondKeyFigureName.PaymentScheduled**

Ordinary payment at settlement date.

Payment Total
^^^^^^^^^^^^^^^
**KeyFigureName.PaymentTotal**

Total payment payed out at the settlement date.

.. math::
    Scheduled Payment + Prepayment.

Outstanding Amount
-------------------
**BondKeyFigureName.OutstandingAmount**

Outstanding amount at the settlement date. Given no buy backs or issuance, this amount should decrease by the amount of
the Total Payment every settlement date.

The key figure **BondKeyFigureName.OutstandingAmountCorrected** represents the outstanding amount 2 business days
before the settlement date.

