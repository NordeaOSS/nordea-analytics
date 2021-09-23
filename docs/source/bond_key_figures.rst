Bond Key Figures
============
This section details the main key figures; what they mean and how they are calculated.
There are fundamentally two different kinds of key figures; stochastic and deterministic ones. The stochastic key figures
are those that require Monte Carlo simulation of interest rate paths and resulting prepayment behavior. The cash flow used is
resulting from prepayments as predicted by the prepayment model. The deterministic ones are those that can be computed
directly from the rate curves and use cash flow with no prepayments.

Most key figures described below are and the stochastic key figures, but the deterministic version of the key figure can
often be retrieved by adding "Det" as prefix to the key figure name, for example;

:KeyFigureName.DetBPVP: `"detbpvp"`

Danish Mortgage bonds with embedded optionality (Callable and Fixed Floaters) are modelled with the prepayment model,
and thus have option adjusted(OA) key figures. In these cases the prepayment model cash flow and the option
adjusted spread(OAS, estimation described below) is used for the estimation. This is embedded where relevant, and thus
does not need to be specified when retrieving the key figure values.

Note, all key figures available in the library are listed in the :class:`KeyFigureName <nordea_analytics.key_figure_name.KeyFigureName>`
class and can be retrieved with the function :meth:`get_key_figures() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_key_figures>`.

Vega
------

:KeyFigureName.Vega: `"vega"`
This key figure shows how much the present value of a bond changes if the
volatility of the rates increase, measured per bond for a one percentage point
volatility change (e.g. kroner per bond per percentage point).

.. math::
    \nu = \frac{\delta}{\delta \sigma_{ATM}} PV

BPV (Basis Point Value)
-----

:KeyFigureName.BPVP: `"bpvp"`
This key figure shows how much the present value of a bond changes if the
rates decrease, measured in currency unit per one percentage point (e.g. kroner per percentage point).

.. math::
    BPV = - \frac{\delta}{\delta R} PV(R)

where `R` is the bumped interest rate curve

CVX (Convexity)
-----

:KeyFigureName.CVXP: `"cvxp"`
This key figure shows how much the BPV of a bond changes if the rates decrease, measured in currency unit per one
percentage point squared (e.g. kroner per percentage point squared).

.. math::
    CVX = \frac{\frac{\delta}{\delta R} PV(R+\Delta) - \frac{\delta}{\delta R} PV(R)}{\Delta}

where `BPV` is the bumped BPV.

Modified duration
------------------

:KeyFigureName.ModifiedDuration: `"modduration"`
This key figure shows how much the present value of a bond changes - per invested currency unit - if the rates decrease
measured in currency unit per one percentage point (e.g. kroner per invested krone per percentage point).

.. math::
    ModDur = \frac{BPV}{\frac{price_{dirty}}{par}}

where :math:`par` is the bond's notional (in most cases 100 DKK) and :math:`price_{dirty}` is bonds quote + accured interest.

Theta
------

:KeyFigureName.Theta: `"theta"`
This key figure measures the drift of the bond price due to the passage of time.

.. math::
    \Theta = (r_{0} + spread) price_{dirty}

Horizon Return
-----------

:KeyFigureName.Return3M: `"return3m"`
:KeyFigureName.Return6M: `"return6m"`
:KeyFigureName.Return12M: `"return12m"`

Forward looking return of holding the bonds for x months while holding all other assumptions fixed.

Historical Return (d/d)
------------------

:KeyFigureName.InternalPriceTotalReturn: `"dd_total_internal"`

The actual one day return of holding the bond.

.. math::
    Retrun_{t(0)} = \frac{price_{dirty,t(1)} - price_{dirty,t(0)}}{price_{dirty,t(0)}}


Accured interest
-----------------

:KeyFigureName.AccruedInterest: `"accint"`
This key figure shows how much interest a bond has accrued since the last coupon payment.

.. math::
    AI=c_{term}{t_{i}/t_{p}}

where :math:`c_{term}` is the coupon for the term in question(e.g.3%=4), :math:`t_{i}` the time in years since last
coupon payment and :math:`t_{p}` the time in years between the last payment and the next.

Present Value (PV)
--------------------

:KeyFigureName.PresentValue: `"present_value"`
The present value of a bond, with a spread of z.

.. math::
    PV(z) = \sum_{t}{E[CF_{t}  exp(-(r_{t} + z)t]}

Spreads
--------

:KeyFigureName.SPREAD: `"spread"`
Difference between the theoretical price and market price, expressed in terms of a spread to the interest rate curve.
The spread of the bond is the spread solving this equation:

.. math::
    PV (spread) = price_{dirty}

replacing `spread` in the PV equation above. Similarly the OA spread is found by solving the equation:

Where relevant, the option adjusted spread(OAS) is simply added to the relevant spread, and the solved for
as above.

.. math::
    PV (OAS) = price_{dirty}

Below are listed other spread key figures, which are computed as described above using the relevant discount factor
in the PV function.

:KeyFigureName.GovSpread: `"gov_spread"`

:KeyFigureName.SwapSpread: `"swap_spread"`

:KeyFigureName.LiborSpread: `"libor_spread`

:KeyFigureName.Libor3mSpread: `"libor_3m_spread"`
Yield Curve Spread (YCS)
-------------------------

:KeyFigureName.YCS: `"ycs"`

Yield curve spreads(YCS) are estimated without taking the prepayment model into account, thus it uses the deterministic
PV for estimation:

.. math::
    PV(z)_{det} = \sum_{t}{E[CF_{t}^{PP=0}] exp(-(r_{t} + z)t}

and as described above the the YCS is estimated as:

.. math::
    PV (YCS) = price_{dirty}

Below are listed other spread key figures, which are computed as described above using the relevant discount factor
in the PV function.

:KeyFigureName.GovYCS: `"govycs"`

:KeyFigureName.SwapYCS: `"swapycs"`

Spread Over Libor (ASW)
------------------------

:KeyFigureName.SpreadOverLibor: `"asw"`

The spread is the pick-up you obtain from swapping the fixed leg into a floating yield compared
to an interbank offered rate. The prepayments are calculated as optimal prepayment behaviour. ASW is only calculated
when the price of the bond is below 100.


Pre-Published payment
------------

:PrePublishedPayment: `"prepublished_prepayment"`

Pre-Published Payment, or prepayments, are extra ordinary payments that happen when a borrower decides to exercise the
prepayment optionality embedded in the Danish Mortgage bond. Prepayments are payed out on settlement date with other
scheduled payments.

The key figure name `PrePayment` represents the pre-published payment amount as a percentage of outstanding amount;

:Prepayment: `"prepayment"`

Preliminary Prepayment
------------------------

:KeyFigureName.PreliminaryPrepayment: `"prelimprepayment"`

The prepayment amount known for the upcoming settlement date. Published weekly, most often on Mondays.

The key figure name `PreliminaryPrepaymentPercentage` represents the preliminary pre payment amount as a percentage
of outstanding amount;

:PreliminaryPrepaymentPercentage: `"ppp"`

Scheduled Payments
-------------------

:KeyFigureName.ScheduledPayment: `"schedpayment"`

Ordinary payment at settlement date.

Total Payment
---------------

:KeyFigureName.TotalPaymentAmt: `"totpaymentamt"`

Total payment payed out at the settlement date.

.. math::
    Scheduled Payment + Prepayment.

Total Payment as percentage of outstanding amount
-------------------------------------------------

:KeyFigureName.TotalPayment: `"totpayment"`

.. math::
    \frac{Total Payment}{Outstanding Amount}

Outstanding Amount
-------------------

:KeyFigureName.OutstandingAmount: `"outstanding_amount"`

Outstanding amount at the settlement date. Given no buy backs or issuance, this amount should decrease by the amount of
the Total Payment every settlement date.

Corrected Outstanding Amount
-----------------------------

:KeyFigureName.CorrectedOutstandingAmount: `"corrected_outstanding_amount"`

Outstanding amount 2 business days before the settlement date.

