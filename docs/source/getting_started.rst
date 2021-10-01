Get started with Nordea Analytics python
=========================================

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
    from nordea_analytics.key_figure_name import BondKeyFigureName

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