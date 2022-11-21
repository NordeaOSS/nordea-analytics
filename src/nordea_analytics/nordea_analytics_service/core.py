"""Core functionality to acces API."""
from datetime import datetime
from typing import Any, Dict, Iterator, List, Union

from nordea_analytics.convention_variable_names import (
    CashflowType,
    DateRollConvention,
    DayCountConvention,
    Exchange,
    TimeConvention,
)
from nordea_analytics.curve_variable_names import (
    CurveDefinitionName,
    CurveName,
    CurveType,
    SpotForward,
)
from nordea_analytics.forecast_names import YieldCountry, YieldHorizon, YieldType
from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    TimeSeriesKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.util import pretty_dict_string
from nordea_analytics.nalib.value_retriever import ValueRetriever
from nordea_analytics.nalib.value_retrievers.BondFinder import BondFinder
from nordea_analytics.nalib.value_retrievers.BondKeyFigureCalculator import (
    BondKeyFigureCalculator,
)
from nordea_analytics.nalib.value_retrievers.BondKeyFigureHorizonCalculator import (
    BondKeyFigureHorizonCalculator,
)
from nordea_analytics.nalib.value_retrievers.BondKeyFigures import BondKeyFigures
from nordea_analytics.nalib.value_retrievers.BondStaticData import BondStaticData
from nordea_analytics.nalib.value_retrievers.Curve import Curve
from nordea_analytics.nalib.value_retrievers.CurveDefinition import CurveDefinition
from nordea_analytics.nalib.value_retrievers.CurveTimeSeries import CurveTimeSeries
from nordea_analytics.nalib.value_retrievers.FXForecast import FXForecast
from nordea_analytics.nalib.value_retrievers.FXQuotes import FXQuotes
from nordea_analytics.nalib.value_retrievers.IndexComposition import IndexComposition
from nordea_analytics.nalib.value_retrievers.LiveBondKeyFigures import (
    LiveBondKeyFigures,
)
from nordea_analytics.nalib.value_retrievers.ShiftDays import ShiftDays
from nordea_analytics.nalib.value_retrievers.TimeSeries import TimeSeries
from nordea_analytics.nalib.value_retrievers.YearFraction import YearFraction
from nordea_analytics.nalib.value_retrievers.YieldForecast import YieldForecast
from nordea_analytics.search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
)


class NordeaAnalyticsCoreService:
    """Main class for the Nordea Analytics python service."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient REST API.
        """
        self._client = client
        self._diagnostic = Dict

    def get_bond_key_figures(
        self,
        symbols: Union[List, str],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
        calc_date: datetime,
        as_df: bool = False,
    ) -> Any:
        """Retrieves given set of key figures for given bonds and calc date.

        Args:
            symbols: List of bonds for which key figures want to be retrieved.
            keyfigures: List of bond key figures which should be retrieved.
                Can be a list of BondKeyFigureName or string.
            calc_date: Date of calculation.
            as_df: Default False. If True, the results are represented as
                pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            BondKeyFigures(self._client, symbols, keyfigures, calc_date), as_df
        )

    def get_index_composition(
        self, indices: Union[List[str], str], calc_date: datetime, as_df: bool = False
    ) -> Any:
        """Retrieves index composition for a given set of indices and calc date.

        Args:
            indices: List of indices for which the
                index composition should be retrieved.
            calc_date: Date of calculation.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            IndexComposition(self._client, indices, calc_date), as_df
        )

    def get_time_series(
        self,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            TimeSeriesKeyFigureName,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
        from_date: datetime,
        to_date: datetime,
        as_df: bool = False,
    ) -> Any:
        """Retrieves historical time series.

        Args:
            symbols: Bonds, swaps, FX, FX swap point.
            keyfigures: Key figure names for request. If symbol is
                something else than a bond, "quote" should be chosen. Can be a
                 list of TimeSeriesKeyFigureName or string.
            from_date: The first date showing historical figures.
            to_date: The last date showing historical figures.
            as_df: if True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            dict containing requested data. if as_df is True,
                the data is in form of a DataFrame
        """
        return self._retrieve_value(
            TimeSeries(self._client, symbols, keyfigures, from_date, to_date), as_df
        )

    def get_curve_time_series(
        self,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        from_date: datetime,
        to_date: datetime,
        tenors: Union[float, List[float]],
        curve_type: Union[str, CurveType] = None,
        time_convention: Union[str, TimeConvention] = None,
        spot_forward: Union[str, SpotForward] = None,
        forward_tenor: float = None,
        as_df: bool = False,
    ) -> Any:
        """Retrieves historical time series of curve points for a given tenor.

        Args:
            curves: Name of curves that should be retrieved.
            from_date: The first date showing historical curve point.
            to_date: The last date showing historical curve point.
            tenors: For what tenors should be curve be constructed,
                at least one tenor is required.
            curve_type: Optional. What type of curve is retrieved.
            time_convention: Optional. Time convention used when curve is constructed.
            spot_forward: Optional. Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Required when spot_forward is set to spot forward or
                implied forward curve.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame
        """
        return self._retrieve_value(
            CurveTimeSeries(
                self._client,
                curves,
                from_date,
                to_date,
                tenors,
                curve_type,
                time_convention,
                spot_forward,
                forward_tenor,
            ),
            as_df,
        )

    def get_curve(
        self,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        calc_date: datetime,
        curve_type: Union[str, CurveType] = None,
        tenor_frequency: float = None,
        time_convention: Union[str, TimeConvention] = None,
        spot_forward: Union[str, SpotForward] = None,
        forward_tenor: float = None,
        as_df: bool = False,
    ) -> Any:
        """Retrieves a curve for a given calculation date.

        Args:
            curves: Name of curves that should be retrieved.
            calc_date: calculation date for request.
            curve_type: Optional. What type of curve is retrieved.
            tenor_frequency: Optional. Frequency between tenors as a fraction of a year.
                Will not have affect unless a specific curve_type are chosen
                like Nelson or Hybrid.
            time_convention: Optional. Time convention used when curve is constructed.
            spot_forward: Optional. Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Required when spot_forward is set to spot forward or
                implied forward curve.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame
        """
        return self._retrieve_value(
            Curve(
                self._client,
                curves,
                calc_date,
                curve_type,
                tenor_frequency,
                time_convention,
                spot_forward,
                forward_tenor,
            ),
            as_df,
        )

    def get_curve_definition(
        self,
        curve: Union[str, CurveDefinitionName, CurveName],
        calc_date: datetime,
        as_df: bool = False,
    ) -> Any:
        """Retrieves a curve definition for a given date.

        Args:
            curve: Name of curve that should be retrieved.
            calc_date: calculation date for request.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            CurveDefinition(self._client, curve, calc_date), as_df
        )

    def get_bond_static_data(
        self,
        symbols: Union[List, str],
        as_df: bool = False,
    ) -> Any:
        """Retrieves latest static data for given ISINs.

        Args:
            symbols: List of bonds for which key figures want to be retrieved.
            as_df: Default False. If True, the results are represented as
                pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(BondStaticData(self._client, symbols), as_df)

    def search_bonds(
        self,
        dmb: bool = False,
        country: str = None,
        currency: str = None,
        issuers: Union[List[Issuers], List[str], Issuers, str] = None,
        asset_types: Union[List[AssetType], List[str], AssetType, str] = None,
        lower_maturity: datetime = None,
        upper_maturity: datetime = None,
        lower_closing_date: datetime = None,
        upper_closing_date: datetime = None,
        lower_coupon: float = None,
        upper_coupon: float = None,
        lower_outstanding_amount: float = None,
        upper_outstanding_amount: float = None,
        amortisation_type: Union[AmortisationType, str] = None,
        capital_centres: Union[
            List[str], str, List[CapitalCentres], CapitalCentres
        ] = None,
        capital_centre_types: Union[
            List[str], str, List[CapitalCentreTypes], CapitalCentreTypes
        ] = None,
        as_df: bool = False,
    ) -> Any:
        """Retrieves bonds given a search criteria.

        Args:
            dmb: Default to False. True if only Danish Mortgage
                Bonds should be found.
            country: Optional. Country of issue.
            currency: Optional. Issue currency.
            issuers: Optional. Name of issuers.
            asset_types: Optional. Type of asset.
            lower_maturity: Optional. Minimum(from) maturity.
            upper_maturity: Optional. Maximum(to) maturity.
            lower_closing_date: Optional. Minimum(from) closing date.
            upper_closing_date: Optional. Maximum(to) closing date.
            lower_coupon: Optional. Minimum coupon.
            upper_coupon: Optional. Maximum coupon.
            amortisation_type: Optional. amortisation type of bond.
            capital_centres: Optional. capital centres names.
                Only relevant for DMB=True.
            capital_centre_types: Optional. capital centres types.
                Only relevant for DMB=True.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.
            lower_outstanding_amount: Optional. Minimum outstanding amount
            upper_outstanding_amount: Optional. Maximum outstanding amount

        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            BondFinder(
                self._client,
                dmb,
                country,
                currency,
                issuers,
                asset_types,
                lower_maturity,
                upper_maturity,
                lower_closing_date,
                upper_closing_date,
                lower_coupon,
                upper_coupon,
                amortisation_type,
                capital_centres,
                capital_centre_types,
                lower_outstanding_amount,
                upper_outstanding_amount,
            ),
            as_df,
        )

    def calculate_bond_key_figure(
        self,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            CalculatedBondKeyFigureName,
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        curves: Union[List[str], str, CurveName, List[CurveName]] = None,
        shift_tenors: Union[List[float], float] = None,
        shift_values: Union[List[float], float] = None,
        pp_speed: float = None,
        prices: Union[float, List[float]] = None,
        spread: float = None,
        spread_curve: Union[str, CurveName] = None,
        yield_input: float = None,
        asw_fix_frequency: str = None,
        ladder_definition: List[float] = None,
        cashflow_type: Union[str, CashflowType] = None,
        as_df: bool = False,
    ) -> Any:
        """Calculate key figures for given bonds and calculation date.

        Args:
            symbols: ISIN or name of bonds that should be valued.
            keyfigures: Bond key figures that should be valued.
            calc_date: Date of calculation
            curves: Optional. Discount curves for calculation
            shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
            shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
            pp_speed: Optional. Prepayment speed. Default = 1.
            prices: Optional. Fixed price per bond
            spread: Optional. Fixed spread for bond. Mandatory to give
                spread_curve also as an input.
            spread_curve: Optional. Spread curve to calculate the
                key figures when a fixed spread is given.
            yield_input: Optional. Fixed yield for bond.
            asw_fix_frequency: Optional. Fixing frequency of swap in ASW calculation.
                Mandatory input in all ASW calculations. "3M", "6M" and "1Y"
                are supported.
            ladder_definition: Optional. Tenors should be included in
                BPV ladder calculation. For example [0.25, 0.5, 1, 3, 5].
            cashflow_type: Optional. Type of cashflow to calculate with.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            BondKeyFigureCalculator(
                self._client,
                symbols,
                keyfigures,
                calc_date,
                curves,
                shift_tenors,
                shift_values,
                pp_speed,
                prices,
                spread,
                spread_curve,
                yield_input,
                asw_fix_frequency,
                ladder_definition,
                cashflow_type,
            ),
            as_df,
        )

    def calculate_horizon_bond_key_figure(
        self,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            HorizonCalculatedBondKeyFigureName,
            List[str],
            List[HorizonCalculatedBondKeyFigureName],
            List[Union[str, HorizonCalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        horizon_date: datetime,
        curves: Union[List[str], str, CurveName, List[CurveName]] = None,
        shift_tenors: Union[List[float], float] = None,
        shift_values: Union[List[float], float] = None,
        pp_speed: float = None,
        prices: Union[float, List[float]] = None,
        cashflow_type: Union[str, CashflowType] = None,
        fixed_prepayments: float = None,
        reinvest_in_series: bool = None,
        reinvestment_rate: float = None,
        spread_change_horizon: float = None,
        align_to_forward_curve: bool = None,
        as_df: bool = False,
    ) -> Any:
        """Calculate future key figures for given bonds.

        Args:
            symbols: ISIN or name of bonds that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation.
            horizon_date: future date of calculation.
            curves: discount curves for calculation.
            shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
            shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
            pp_speed: Prepayment speed. Default = 1.
            cashflow_type: Optional. Type of cashflow to calculate with.
            prices: fixed price per bond.
            fixed_prepayments: Prepayments between calc_cate and horizon date.
                Value of 0.01 would mean that prepayments are set to 1%,
                but model prepayments are still used after horizon date.
                If noting entered, then model prepayments used.
            reinvest_in_series: True if you want to reinvest in the series.
                Default value is True.
            reinvestment_rate: Rate you want to reinvest if you don't
                want to reinvest in series. Only relevant if
                    reinvest_in_series is False, or horizon date is
                    further out than maturity of the bond.
            spread_change_horizon: bump the spread between calc date
                and horizon date. Value should be in bps.
            align_to_forward_curve: True if you want the curve used for horizon
                calculations to be the respective forward curve.
                Default is False.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            BondKeyFigureHorizonCalculator(
                self._client,
                symbols,
                keyfigures,
                calc_date,
                horizon_date,
                curves,
                shift_tenors,
                shift_values,
                pp_speed,
                prices,
                cashflow_type,
                fixed_prepayments,
                reinvest_in_series,
                reinvestment_rate,
                spread_change_horizon,
                align_to_forward_curve,
            ),
            as_df,
        )

    def get_fx_forecasts(
        self,
        currency_pair: str,
        as_df: bool = False,
    ) -> Any:
        """Retrieves Nordea's latest FX forecasts.

        Args:
            currency_pair: The currency cross for which forecast is retrieved.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(FXForecast(self._client, currency_pair), as_df)

    def get_yield_forecasts(
        self,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
        as_df: bool = False,
    ) -> Any:
        """Retrieves Nordea's latest yield forecasts.

        Args:
            country: The country for which forecast is retrieved.
            yield_type: The type of yield for which forecast is retrieved.
            yield_horizon: The horizon for which forecast is retrieved.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        return self._retrieve_value(
            YieldForecast(self._client, country, yield_type, yield_horizon), as_df
        )

    def get_shift_days(
        self,
        date: datetime,
        days: int,
        exchange: Union[str, Exchange] = None,
        day_count_convention: Union[str, DayCountConvention] = None,
        date_roll_convention: Union[str, DateRollConvention] = None,
    ) -> datetime:
        """Shifts a date using internal holiday calendars.

        Args:
            date: The date that will be shifted.
            days: The number of days to shift 'date' with.
                Negative values move date back in time.
            exchange: The exchange's holiday calendar will be used.
            day_count_convention: The convention to use for counting days.
            date_roll_convention: The convention to use for rolling
                when a holiday is encountered.

        Returns:
            The shifted datetime is returned.
        """
        return ShiftDays(
            self._client,
            date,
            days,
            exchange,
            day_count_convention,
            date_roll_convention,
        ).to_datetime()

    def get_year_fraction(
        self,
        from_date: datetime,
        to_date: datetime,
        time_convention: Union[str, TimeConvention],
    ) -> float:
        """Calculate the time between two dates in terms of years.

        Args:
            from_date: The start date of the time calculation.
            to_date: The end date of the time calculation.
            time_convention: The convention to use for counting time.

        Returns:
            The time in decimal years between the start and end date.
        """
        return YearFraction(
            self._client,
            from_date,
            to_date,
            time_convention,
        ).to_float()

    def get_bond_live_key_figures(
        self,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        as_df: bool = False,
    ) -> Any:
        """Return last available live bond key figures for a given ISINs.

        Args:
            symbols: ISIN or name of bonds that should be retrieved live.
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary

        Returns:
            pandas DataFrame or dictionary

        """
        return self._retrieve_value(
            LiveBondKeyFigures(symbols, self._client, keyfigures, as_df), as_df
        )

    def iter_live_bond_key_figures(
        self,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        as_df: bool = False,
    ) -> Iterator[Any]:
        """Subscribe for live bond keyfigures updates from a server.

        Args:
            symbols: ISIN or name of bonds that should be retrieved live.
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary

        Returns:
            Iterator object that can be used to iterate over HTTP stream.
        """
        return LiveBondKeyFigures(
            symbols, self._client, keyfigures, as_df
        ).stream_keyfigures

    def dump_diagnostic(self) -> str:
        """Return the diagnostic information in a pretty way."""
        return pretty_dict_string(self._client.diagnostic)

    @staticmethod
    def _retrieve_value(value_retriever: ValueRetriever, as_df: bool = False) -> Any:
        if as_df:
            return value_retriever.to_df()
        return value_retriever.to_dict()
