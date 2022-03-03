from datetime import datetime
from typing import Any, Dict, List, Union

from nordea_analytics.curve_variable_names import (
    CurveName,
    CurveType,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    TimeSeriesKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
    LiveDataRetrievalServiceClient,
)
from nordea_analytics.nalib.streaming_service import StreamListener
from nordea_analytics.nalib.value_retriever import (
    BondFinder,
    BondKeyFigureCalculator,
    BondKeyFigureHorizonCalculator,
    BondKeyFigures,
    Curve,
    CurveDefinition,
    CurveTimeSeries,
    FXForecast,
    IndexComposition,
    LiveBondKeyFigures,
    TimeSeries,
)
from nordea_analytics.search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
)


class NordeaAnalyticsService:
    """Main class for the Nordea Analytics python service."""

    def __init__(
        self,
        client: DataRetrievalServiceClient = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile; REST API.
        """
        self._client = DataRetrievalServiceClient() if client is None else client

    def get_bond_key_figures(
        self,
        isins: Union[List, str],
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
        """Retrieves given set of key figures for given ISINs and calc date.

        Args:
            isins: List of ISINs for which key figures want to be retrieved.
            keyfigures: List of bond key figures which should be retrieved.
                Can be a list of BondKeyFigureName or string.
            calc_date: Date of calculation.
            as_df: Default False. If True, the results are represented as
                pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return BondKeyFigures(self._client, isins, keyfigures, calc_date).to_df()
        else:
            return BondKeyFigures(self._client, isins, keyfigures, calc_date).to_dict()

    def get_index_composition(
        self, indices: Union[List[str], str], calc_date: datetime, as_df: bool = False
    ) -> Any:
        """Retrieves index composition for a given set og indices and calc date.

        Args:
            indices: List of Indices for which the
                index composition should be retrieved.
            calc_date: Date of calculation.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return IndexComposition(self._client, indices, calc_date).to_df()
        else:
            return IndexComposition(self._client, indices, calc_date).to_dict()

    def get_time_series(
        self,
        symbol: Union[str, List[str]],
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
            symbol: Bonds ISINs, swaps, FX, FX swap point.
            keyfigures: Key figure names for request. If symbol is
                something else than a bond ISIN, "quote" should be chosen. Can be a
                 list of BondKeyFigureName or string.
            from_date: The first date showing historical figures.
            to_date: The last date showing historical figures.
            as_df: if True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            dict containing requested data. if as_df is True,
                the data is in form of a DataFrame
        """
        if as_df:
            return TimeSeries(
                self._client, symbol, keyfigures, from_date, to_date
            ).to_df()
        else:
            return TimeSeries(
                self._client, symbol, keyfigures, from_date, to_date
            ).to_dict()

    def get_curve_time_series(
        self,
        curve: Union[str, CurveName],
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
            curve: Name of curve that should be retrieved.
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
        if as_df:
            return CurveTimeSeries(
                self._client,
                curve,
                from_date,
                to_date,
                tenors,
                curve_type,
                time_convention,
                spot_forward,
                forward_tenor,
            ).to_df()
        else:
            return CurveTimeSeries(
                self._client,
                curve,
                from_date,
                to_date,
                tenors,
                curve_type,
                time_convention,
                spot_forward,
                forward_tenor,
            ).to_dict()

    def get_curve(
        self,
        curve: Union[str, CurveName],
        calc_date: datetime,
        curve_type: Union[str, CurveType] = None,
        tenor_frequency: float = None,
        time_convention: Union[str, TimeConvention] = None,
        spot_forward: Union[str, SpotForward] = None,
        forward_tenor: float = None,
        as_df: bool = False,
    ) -> Any:
        """Retrieves a curve for a given tenor frequency.

        Args:
            curve: Name of curve that should be retrieved.
            calc_date: calculation date for request.
            curve_type: Optional. What type of curve is retrieved.
            tenor_frequency: Optional. Frequency between tenors as a fraction of a year.
                Will not have affect unless a certain curve_type are chosen
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
        if as_df:
            return Curve(
                self._client,
                curve,
                calc_date,
                curve_type,
                tenor_frequency,
                time_convention,
                spot_forward,
                forward_tenor,
            ).to_df()
        else:
            return Curve(
                self._client,
                curve,
                calc_date,
                curve_type,
                tenor_frequency,
                time_convention,
                spot_forward,
                forward_tenor,
            ).to_dict()

    def get_curve_definition(
        self,
        curve: Union[str, CurveName],
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
        if as_df:
            return CurveDefinition(self._client, curve, calc_date).to_df()
        else:
            return CurveDefinition(self._client, curve, calc_date).to_dict()

    def search_bonds(
        self,
        dmb: bool = False,
        country: str = None,
        currency: str = None,
        issuers: Union[List[Issuers], List[str], Issuers, str] = None,
        asset_types: Union[List[AssetType], List[str], AssetType, str] = None,
        lower_maturity: datetime = None,
        upper_maturity: datetime = None,
        lower_coupon: float = None,
        upper_coupon: float = None,
        amortisation_type: Union[AmortisationType, str] = None,
        is_io: bool = None,
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
            lower_coupon: Optional. Minimum coupon.
            upper_coupon: Optional. maximum coupon.
            amortisation_type: Optional. amortisation type of bond.
            is_io: Optional. Is Interest Only - only relevant for DMB.
            capital_centres: Optional. capital centres names
                - only relevant for DMB.
            capital_centre_types: Optional. capital centres types
                - only relevant for DMB.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return BondFinder(
                self._client,
                dmb,
                country,
                currency,
                issuers,
                asset_types,
                lower_maturity,
                upper_maturity,
                lower_coupon,
                upper_coupon,
                amortisation_type,
                is_io,
                capital_centres,
                capital_centre_types,
            ).to_df()
        else:
            return BondFinder(
                self._client,
                dmb,
                country,
                currency,
                issuers,
                asset_types,
                lower_maturity,
                upper_maturity,
                lower_coupon,
                upper_coupon,
                amortisation_type,
                is_io,
                capital_centres,
                capital_centre_types,
            ).to_dict()

    def calculate_bond_key_figure(
        self,
        isin: str,
        keyfigures: Union[
            str,
            CalculatedBondKeyFigureName,
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        curves: Union[List[str], str, CurveName, List[CurveName]] = None,
        rates_shifts: Union[List[str], str] = None,
        pp_speed: float = None,
        price: float = None,
        spread: float = None,
        spread_curve: Union[str, CurveName] = None,
        asw_fix_frequency: str = None,
        ladder_definition: List[str] = None,
        as_df: bool = False,
    ) -> Any:
        """Calculate bond key figure for given isin.

        Args:
            isin: ISIN of bond that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: Date of calculation
            curves: Optional. Discount curves for calculation
            rates_shifts: Optional. Shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Optional. Prepayment speed. Default = 1.
            price: Optional. Fixed price for ISIN
            spread: Optional. Fixed spread for ISIN. Mandatory to give
                spread_curve also as an input.
            spread_curve: Optional. Spread curve to calculate the
                key figures when a fixed spread is given.
            asw_fix_frequency: Optional. Fixing frequency of swap in ASW calculation.
                Mandatory input in all ASW calculations.
            ladder_definition: Optional. What tenors should be included in
                BPV ladder calculation.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return BondKeyFigureCalculator(
                self._client,
                isin,
                keyfigures,
                calc_date,
                curves,
                rates_shifts,
                pp_speed,
                price,
                spread,
                spread_curve,
                asw_fix_frequency,
                ladder_definition,
            ).to_df()
        else:
            return BondKeyFigureCalculator(
                self._client,
                isin,
                keyfigures,
                calc_date,
                curves,
                rates_shifts,
                pp_speed,
                price,
                spread,
                spread_curve,
                asw_fix_frequency,
                ladder_definition,
            ).to_dict()

    def calculate_horizon_bond_key_figure(
        self,
        isin: str,
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
        rates_shifts: Union[List[str], str] = None,
        pp_speed: float = None,
        price: float = None,
        fixed_prepayments: float = None,
        reinvest_in_series: bool = None,
        reinvestment_rate: float = None,
        spread_change_horizon: float = None,
        as_df: bool = False,
    ) -> Any:
        """Calculate future bond key figure for given isin.

        Args:
            isin: ISIN of bond that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation.
            horizon_date: future date of calculation.
            curves: discount curves for calculation.
            rates_shifts: shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Prepayment speed. Default = 1.
            price: fixed price for ISIN.
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
                and horizon date. Values should be in bps.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary


        Returns:
            Dictionary containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return BondKeyFigureHorizonCalculator(
                self._client,
                isin,
                keyfigures,
                calc_date,
                horizon_date,
                curves,
                rates_shifts,
                pp_speed,
                price,
                fixed_prepayments,
                reinvest_in_series,
                reinvestment_rate,
                spread_change_horizon,
            ).to_df()
        else:
            return BondKeyFigureHorizonCalculator(
                self._client,
                isin,
                keyfigures,
                calc_date,
                horizon_date,
                curves,
                rates_shifts,
                pp_speed,
                price,
                fixed_prepayments,
                reinvest_in_series,
                reinvestment_rate,
                spread_change_horizon,
            ).to_dict()

    def get_fx_forecasts(
        self,
        currency_pair: str,
        as_df: bool = False,
    ) -> Any:
        """Retrieves given set of key figures for given ISINs and calc date.

        Args:
            currency_pair: The currency cross for which forecast is retrieved.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary.

        Returns:
            Dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return FXForecast(self._client, currency_pair).to_df()
        else:
            return FXForecast(self._client, currency_pair).to_dict()


class NordeaAnalyticsLiveService:
    """Main class for the Nordea Analytics python live service."""

    def __init__(
        self,
        client: LiveDataRetrievalServiceClient = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile; REST API.
        """
        self._client = LiveDataRetrievalServiceClient() if client is None else client
        self.dict: Dict = {}

    def get_live_bond_key_figures(
        self,
        isins: Union[str, List[str]],
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        as_df: bool = False,
    ) -> StreamListener:
        """Stream live bond key figures for a given ISINs.

        Args:
            isins: ISINs of bond that should be retrieved live.
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            as_df: Default False. If True, the results are represented
                as pandas DataFrame, else as dictionary

        Returns:
            LiveBondKeyFigures class

        """
        return LiveBondKeyFigures(
            self._client, isins, keyfigures, self.dict, as_df
        ).get_live_streamer()
