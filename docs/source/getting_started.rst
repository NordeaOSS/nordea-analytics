Get started with Nordea Analytics python
=========================================

Install
-----------
Run: `pip install nordea-analytics`

Start coding with Nordea Analytics python
------------------------------------------

All methods available in the Nordea Analytics python can be retrieved through the
NordeaAnalyticsService and NordeaAnalyticsLiveService classes.

.. code-block:: python

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsLiveService

Available methods
^^^^^^^^^^^^^^^^^^^^
All methods can return results in the form of a dictionary(default) or as a pandas DataFrame(as_df=True).

From NordeaAnalyticsService:

* :meth:`get_bond_key_figures() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_bond_key_figures>`.
* :meth:`get_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_time_series>`.
* :meth:`get_index_composition() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_index_composition>`.
* :meth:`get_curve_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_curve_time_series>`
* :meth:`get_curve() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_curve>`
* :meth:`get_curve_definition() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_curve_definition>`
* :meth:`search_bonds() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.search_bonds>`
* :meth:`calculate_bond_key_figure() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.calculate_bond_key_figure>`
* :meth:`calculate_horizon_bond_key_figure() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.calculate_horizon_bond_key_figure>`

From NordeaAnalyticsService:

* :meth:`get_live_bond_key_figures() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsLiveService.get_live_bond_key_figures>`.

Enumeration classes for input parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Many input parameters are controlled by enumeration classes. These are the following available:

From `nordea_analytics.key_figure_name`

* :meth:`BondKeyFigureName <nordea_analytics.key_figure_names.BondKeyFigureName>`
* :meth:`TimeSeriesKeyFigureName <nordea_analytics.key_figure_names.TimeSeriesKeyFigureName>`
* :meth:`CalculatedBondKeyFigureName <nordea_analytics.key_figure_names.CalculatedBondKeyFigureName>`
* :meth:`HorizonCalculatedBondKeyFigureName <nordea_analytics.key_figure_names.HorizonCalculatedBondKeyFigureName>`
* :meth:`LiveBondKeyFigureName <nordea_analytics.key_figure_names.LiveBondKeyFigureName>`

From `nordea_analytics.curve_variable_names`

* :meth:`CurveName <nordea_analytics.curve_variable_names.CurveName>`
* :meth:`CurveType <nordea_analytics.curve_variable_names.CurveType>`
* :meth:`TimeConvention <nordea_analytics.curve_variable_names.TimeConvention>`
* :meth:`SpotForward <nordea_analytics.curve_variable_names.SpotForward>`

from `nordea_analytics.search_bond_names`

* :meth:`AmortisationType <nordea_analytics.search_bond_names.AmortisationType>`
* :meth:`AssetType <nordea_analytics.search_bond_names.AssetType>`
* :meth:`CapitalCentres <nordea_analytics.search_bond_names.CapitalCentres>`
* :meth:`CapitalCentreTypes <nordea_analytics.search_bond_names.CapitalCentreTypes>`
* :meth:`Issuers <nordea_analytics.search_bond_names.Issuers>`


Basic examples
---------------
Get Bond Key Figures
^^^^^^^^^^^^^^^^^^^^^
The following example retrieves Vega, BPV and CVX for a given set of ISINs and returns the results in a pandas DataFrame.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.key_figure_names import BondKeyFigureName

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime.today() - datetime.timedelta(1)
    isins =['DK0002000421', 'DK0002004092', 'DK0002013408', 'DK0006344171']
    bond_key_figure_name = [BondKeyFigureName.Vega, BondKeyFigureName.BPVP, BondKeyFigureName.CVXP]

    bond_key_figures = na_service.get_bond_key_figures(isins, bond_key_figure_name,
                                                   value_date, as_df=True)


Get Time Series
^^^^^^^^^^^^^^^^
The following example retrieves daily Vega, BPV and Convexity for a given set of ISINs for the time period 1st of
January 2021 to the day to day and returns the results in a python dictionary. The
:meth:`get_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_time_series>` function
can also retrieve time series for swaps, FX, FX swap point, then the key figure name should be `TimeSeriesKeyFigureName.Quote`.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.key_figure_names import TimeSeriesKeyFigureName

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2021, 1, 1)
    to_date = datetime.datetime.today()
    symbols = ['DK0002000421', 'DK0002004092', 'DK0002013408', 'DK0006344171']
    key_figure_name = [TimeSeriesKeyFigureName.Vega, TimeSeriesKeyFigureName.BPVP,
                       TimeSeriesKeyFigureName.CVXP]

    time_Series = na_service.get_time_series(symbols, key_figure_name, from_date, to_date)

Get Index Composition
^^^^^^^^^^^^^^^^^^^^^^
The following example retrieves index composition for a set of Indices for the value date today, and returns the result
in a pandas DataFrame.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

    na_service = NordeaAnalyticsService()
    calc_date = datetime.datetime.today() - datetime.timedelta(1)
    indices = ['DK Mtg Callable', 'DK Govt']

    index_composition = na_service.get_index_composition(indices, calc_date, as_df=True)

Get Curve Time Series
^^^^^^^^^^^^^^^^^^^^^^
The following example retrieves daily points on the 0.5Y and 1Y `DKKSWAP` spot par curve for the time period 1st of
January 2021 to the day to day and returns the results in a pandas DataFrame. The curve is constructed using time
convention 30/360.

.. code-block:: python


    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveName, CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2021, 1, 1)
    to_date = datetime.datetime.today()
    curve = CurveName.DKKSWAP
    tenors = [1, 0.5]  # at least one required.
    curve_type = CurveType.ParCurve  # Optional input
    time_convention = TimeConvention.TC_30360  # Optional input
    curve_time_series = na_service.get_curve_time_series(curve, from_date, to_date, tenors,
                                                         curve_type=curve_type,
                                                         time_convention=time_convention, as_df=True)

The following example retrieves daily points on the 2Y1Y `EURGOV` forward curve, for the time period 3rd of
January 2021 to the day to day and returns the results in a pandas DataFrame. The curve is constructed using the
bootstrap method time convention 30/360. Note, when forward or implied forward curves are retrieved, a forward tenor
has to be given.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveName,  CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2022, 1, 3)
    to_date = datetime.datetime.today()
    curve = CurveName.EURGOV
    tenors = 1
    curve_type = CurveType.Bootstrap  # Optional input
    time_convention = TimeConvention.Act365  # Optional input
    spot_forward = SpotForward.Forward  # Optional input
    forward_tenor = 2  # Required when spot_forward is set to spot forward or implied forward curve.
    curve_time_series = na_service.get_curve_time_series(curve, from_date, to_date, tenors,
                                                         curve_type=curve_type,
                                                         time_convention=time_convention,
                                                         spot_forward=spot_forward,
                                                         forward_tenor=forward_tenor,
                                                         as_df=True)

Get Curve
^^^^^^^^^
The following example retrieves the `DKKSWAP Libor` spot par curve with for the value date
3rd of January 20222 and returns the results in a pandas DataFrame.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveName

    na_service = NordeaAnalyticsService()
    calc_date = datetime.datetime(2022, 1, 3)
    curve_name = CurveName.DKKSWAP_Libor
    curve = na_service.get_curve(curve_name, calc_date, as_df=True)

The following example retrieves the `USDGOV` 2Y forward curve with a half-year tenor interval (0.5) for the value date
1st January 2021 and returns the results in a pandas DataFrame. The curve is constructed using the
Nelson Siegel method and time convention Act/365.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveName, CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime(2021, 1, 4)
    curve_name = CurveName.USDGOV
    curve_type = CurveType.NelsonSiegel
    tenor_frequency = 0.5
    time_convention = TimeConvention.Act365
    spot_forward = SpotForward.Forward
    forward_tenor = 2

    curve = na_service.get_curve(curve_name, value_date, curve_type=curve_type,
                                 tenor_frequency=tenor_frequency,
                                 time_convention=time_convention, spot_forward=spot_forward,
                                 forward_tenor=forward_tenor, as_df=True)

Note that tenor frequency input will not have affect unless a certain curve_type are chosen like Nelson or Hybrid.

Get Curve Definition
^^^^^^^^^^^^^^^^^^^^
The following example shows the curve definition (bonds, quotes, weights and maturities contributing
to the curve) of the `EURGOV` curve for the value date of 1st of January 2021.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveName

    na_service = NordeaAnalyticsService()
    calc_date = datetime.datetime(2021, 1, 1)
    curve_name = CurveName.EURGOV
    curve_def = na_service.get_curve_definition(curve_name, calc_date, as_df=True)

Search Bonds
^^^^^^^^^^^^^
The search_bonds() function requires at least one search criteria.
The following example returns list of ISINs and bond names for USD Fixed to Float Bond with annuity as amortisation
type. The results are in a DataFrame format.

.. code-block:: python

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.search_bond_names import AssetType, AmortisationType

    na_service = NordeaAnalyticsService()
    currency = "USD"
    asset_type = AssetType.FixToFloatBond
    amortisation_type = AmortisationType.Annuity

    df = na_service.search_bonds(currency=currency, asset_types=asset_type,
                             amortisation_type=amortisation_type, as_df=True)

The following example returns list of ISINs and bond names for `only` Danish Mortgage Bonds (dmb=True), with DKK as currency and maturity between 9th
of December 2021 to the day to day. Note that if dmb=False (default value), it would return `all` bonds with the same criteria,
including Danish Mortgage Bonds. The results are in a DataFrame format.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

    na_service = NordeaAnalyticsService()
    from_maturity = datetime.datetime(2021, 12, 9)
    to_maturity = datetime.datetime.today()
    currency = "DKK"

    df = na_service.search_bonds(dmb=True, currency=currency,
                             upper_maturity=to_maturity, lower_maturity=from_maturity,
                             as_df=True)

When asset_type is set to Danish Capped Floaters, then both capped floaters and normal floaters are returned.
To search specifically for capped floaters set upper_coupon = 1,000 (shown in example below).
To search specifically for normal floaters set lower_coupon = 100,000.

.. code-block:: python

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.search_bond_names import AssetType

    na_service = NordeaAnalyticsService()
    asset_type = AssetType.DanishCappedFloaters
    upper_coupon = 1000


    currency = "DKK"

    df = na_service.search_bonds(dmb=True, currency=currency, asset_types=asset_type,
                             upper_coupon=upper_coupon, as_df=True)

Other serach criterias are listed in :meth:`search_bonds()
<nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.search_bonds>`

Calculate Bond Key Figure
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following example calculates the spread and bpv for the ISIN `DK0002000421` at 15th of January 2021.
The returned DataFrame shows results for both given discount curves, `DKKSWAP Disc OIS` and `DKKSWAP Libor`, where they
are shifted up by 5 bps on the 6M, 1Y and 2Y tenor.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.key_figure_names import CalculatedBondKeyFigureName
    from nordea_analytics.curve_variable_names import CurveName

    na_service = NordeaAnalyticsService()
    isin = 'DK0002000421'
    bond_key_figure = [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.BPV]
    calc_date = datetime.datetime(2021, 12, 15)
    curves = [CurveName.DKKSWAP_Disc_OIS, CurveName.DKKSWAP_Libor] #Optional
    rates_shifts = ["6M 5", "1Y 5", "2Y 5"] #Optional
    df = na_service.calculate_bond_key_figure(isin, bond_key_figure, calc_date, curves=curves,
                                          rates_shifts=rates_shifts, as_df=True)

Other optional input variables can be found in :meth:`calculate_bond_key_figure()
<nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.calculate_bond_key_figure>`

Calculate Horizon Bond Key Figure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following example calculates the BPV, CVX and Spread for the future date 18th of February 2022, given information
at 14th of February 2022 for the ISIN `DK0002000421`. Key figure "Price" shows the price at
14th of February 2022.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.key_figure_names import HorizonCalculatedBondKeyFigureName

    na_service = NordeaAnalyticsService()
    isin = 'DK0002000421'
    bond_key_figure = [HorizonCalculatedBondKeyFigureName.BPV, HorizonCalculatedBondKeyFigureName.CVX,
               HorizonCalculatedBondKeyFigureName.Spread, HorizonCalculatedBondKeyFigureName.Price]
    calc_date = datetime.datetime(2022, 2, 14)
    horizon_date = datetime.datetime(2022, 2, 18)
    df = na_service.calculate_horizon_bond_key_figure(isin,
                                                      bond_key_figure, calc_date,
                                                      horizon_date, as_df=True)

Other optional input variables can be found in :meth:`calculate_horizon_bond_key_figure()
<nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.calculate_horizon_bond_key_figure>`

Get Live Key Figure
^^^^^^^^^^^^^^^^^^^^^^
The following example returns live Quotes and CVX in a pandas DataFrame format and stops the feed after one minute.

.. code-block:: python

    from nordea_analytics.key_figure_names import LiveBondKeyFigureName
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsLiveService
    import time

    live_service = NordeaAnalyticsLiveService()
    live_bond_keyfigure = live_service.get_live_bond_key_figures(['HU0000523980', 'SGXZ94462934'],
                                                             [LiveBondKeyFigureName.Quote,
                                                              LiveBondKeyFigureName.CVX],
                                                             as_df=True)
    t_end = time.time() + 60 * 1  #one minute
    with live_bond_keyfigure as live_streamer:
        while live_streamer:
            df = live_streamer.run()
            print(df)
            if time.time() > t_end:
                live_streamer.stop()

