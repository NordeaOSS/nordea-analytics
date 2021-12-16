Get started with Nordea Analytics python
=========================================

Getting access
---------------
Access is needed to retrieve the data in the Nordea Analytics python library. In order to get the access please reach out to market.data@nordea.com.

Install
-----------
Run: `pip install nordea-analytics`

Start coding with Nordea Analytics python
------------------------------------------

All methods available in the Nordea Analytics python can be retrieved through the
NordeaAnalyticsService class.

.. code-block:: python

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

It is beneficial to import the enumeration classes where relevant, as it contains all available variable names and is user friendly.

For bond key figures;

.. code-block:: python

    from nordea_analytics.bond_key_figure_name import BondKeyFigureName

For variable names when getting curves;

.. code-block:: python

    from nordea_analytics.curve_variable_names import CurveType, SpotForward, TimeConvention

Available methods
^^^^^^^^^^^^^^^^^^^^
Currently 3 methods are available in the Nordea Analytics python tool. All methods
can return results in the form of a dictionary(default) or as a pandas DataFrame.

* :meth:`get_bond_key_figures() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_bond_key_figures>`.
* :meth:`get_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_time_series>`.
* :meth:`get_index_composition() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_index_composition>`.
* :meth:`get_curve_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_curve_time_series>`

Basic examples
---------------
Get Bond Key Figures
^^^^^^^^^^^^^^^^^^^^^
The following example retrieves Vega, BPV and CVX for a given set of ISINs and returns the results in a pandas DataFrame.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.bond_key_figure_name import BondKeyFigureName

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime.today() - datetime.timedelta(1)
    isins=['DK0002000421', 'DK0002004092', 'DK0002013408', 'DK0006344171']
    bond_key_figure_name = [BondKeyFigureName.Vega, BondKeyFigureName.BPVP, BondKeyFigureName.CVXP]

    bond_key_figures = na_service.get_bond_key_figures(isins, bond_key_figure_name, value_date, as_df=True)


Get Time Series
^^^^^^^^^^^^^^^^
The following example retrieves daily Vega, BPV and Convexity for a given set of ISINs for the time period 1st of
January 2021 to the day to day and returns the results in a python dictionary. The
:meth:`get_time_series() <nordea_analytics.nordea_analytics_service.NordeaAnalyticsService.get_time_series>` function
can also retrieve time series for swaps, FX, FX swap point, then the bond key figure name should be `BondKeyFigureName.Quote`.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.bond_key_figure_name import BondKeyFigureName

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2021, 1, 1)
    to_date = datetime.datetime.today()
    isins = ['DK0002000421', 'DK0002004092', 'DK0002013408', 'DK0006344171']
    bond_key_figure_name = [BondKeyFigureName.Vega, BondKeyFigureName.BPVP, BondKeyFigureName.CVXP]

    time_Series = na_service.get_time_series(isins, bond_key_figure_name, from_date, to_date)

Get Index Composition
^^^^^^^^^^^^^^^^^^^^^^
The following example retrieves index composition for a set of Indices for the value date today, and returns the result
in a pandas DataFrame.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime.today() - datetime.timedelta(1)
    indices = ['DK Mtg Callable', 'DK Govt']

    index_composition = na_service.get_index_composition(indices, value_date, as_df=True)

Get Curve Time Series
^^^^^^^^^^^^^^^^^^^^^^
The following example retrieves daily points on the 0.5Y and 1Y `DKKSWAP` spot par curve for the time period 1st of
January 2021 to the day to day and returns the results in a pandas DataFrame. The curve is constructed using time
convention 30/360.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2021, 1, 1)
    to_date = datetime.datetime.today()
    curve = 'DKKSWAP'
    curve_type= CurveType.ParCurve
    tenors= [1, 0.5]
    time_convention = TimeConvention.TC_30360
    spot_forward = SpotForward.Spot
    curve_time_series = na_service.get_curve_time_series(curve, from_date, to_date, curve_type,
        time_convention, tenors, spot_forward, as_df=True)


The following example retrieves daily points on the 2Y1Y `DKKSWAP` forward curve, for the time period 1st of
January 2021 to the day to day and returns the results in a pandas DataFrame. The curve is constructed using the
bootstrap method time convention 30/360. Note, when forward or implied forward curves are retrieved, a forward tenor
has to be given.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    from_date = datetime.datetime(2021, 1, 1)
    to_date = datetime.datetime.today()
    curve = 'DKKSWAP'
    curve_type= CurveType.Bootstrap
    tenors= [1]
    time_convention = TimeConvention.Act365
    spot_forward = SpotForward.Forward
    curve_time_series = na_service.get_curve_time_series(curve, from_date, to_date, curve_type,
        time_convention, tenors, spot_forward, forward_tenor=2, as_df=True)

Get Curve
^^^^^^^^^
The following example retrieves the `DKKSWAP` spot par curve with a half-year tenor interval (0.5) for the value date
1st January 2021 and returns the results in a pandas DataFrame. The curve is constructed using the
bootstrap method time convention Act/365.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.curve_variable_names import CurveType, TimeConvention, SpotForward

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime(2021, 1, 1)
    curve_name = 'DKKSWAP'
    curve_type= CurveType.Bootstrap
    time_convention = TimeConvention.Act365
    spot_forward = SpotForward.Spot
    tenor_frequency = 0.5
    curve = na_service.get_curve(curve_name, value_date, tenor_frequency, curve_type,
                                 time_convention, spot_forward, as_df=True)


Get Curve Definition
^^^^^^^^^^^^^^^^^^^^
The following example shows the curve definition (bonds, quotes, weights and maturities contributing
to the curve) of the `EURGOV` curve for the value date of 1st of January 2021.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

    na_service = NordeaAnalyticsService()
    value_date = datetime.datetime(2021, 1, 1)
    curve_name = 'EURGOV'
    curve_def = na_service.get_curve_definition(curve_name, value_date, as_df=True)

Search Bonds
^^^^^^^^^^^^^
The following example returns list of ISINs and bond names for USD Fixed to Float Bond with annuity as amortisation
type. The results are in a DataFrame format.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.search_bond_names import AssetType, AmortisationType

    na_service = NordeaAnalyticsService()
    currency = "USD"
    asset_type = AssetType.FixToFloatBond
    amortisation_type = AmortisationType.Annuity

    df = na_service.search_bonds(dmb=False, currency=currency, asset_types=asset_type,
                                  amortisation_type=amortisation_type, as_df=True)

The following example returns list of ISINs and bond names for `only` Danish Mortgage Bonds (dmb=True), with DKK as currency and maturity between 9th
of December 2021 to the day to day. Note that if dmb=False, the it would return `all` bonds with the same criteria,
including Danish Mortgage Bonds. The results are in a DataFrame format.

.. code-block:: python

    import datetime

    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.search_bond_names import AssetType

    na_service = NordeaAnalyticsService()
    from_maturity = datetime.datetime(2021, 12, 9)
    to_maturity = datetime.datetime.today()
    currency = "DKK"

    df = na_service.search_bonds(dmb=True, currency=currency, upper_maturity=to_maturity, lower_maturity=from_maturity,
                                 as_df=True)

Calculate Bond Key Figure
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following example calculates the spread for the ISIN `DK0002000421` at 15th of January 2021.
The returned DataFrame shows results for both given discount curves, `DKKSWAP Disc OIS` and `DKKSWAP Libor`, where they
are shifted up by 5 bps on the 6M, 1Y and 2Y tenor.

.. code-block:: python

    import datetime
    from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService
    from nordea_analytics.bond_key_figure_name import CalculatedBondKeyFigureNames

    na_service = NordeaAnalyticsService()
    isin = 'DK0002000421'
    bond_key_figure = CalculatedBondKeyFigureNames.Spread
    calc_date = datetime.datetime(2021, 12, 15)
    curves = ["DKKSWAP Disc OIS", "DKKSWAP Libor"]
    rates_shifts = ["6M 5", "1Y 5", "2Y 5"]
    df = na_service.calculate_bond_key_figure(isin, bond_key_figure, calc_date, curves=curves,
                                              rates_shifts=rates_shifts, as_df=True)
