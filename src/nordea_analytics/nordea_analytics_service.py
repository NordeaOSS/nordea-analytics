from datetime import datetime
from typing import Any, List, Union

from nordea_analytics.bond_key_figure_name import BondKeyFigureName
from nordea_analytics.curve_variable_names import (
    CurveType,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.value_retriever import (
    BondKeyFigures,
    CurveTimeSeries,
    IndexComposition,
    TimeSeries,
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
        isins: List,
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
        ],
        calc_date: datetime,
        as_df: bool = False,
    ) -> Any:
        """Retrieves given set of kewy figures for given ISINs and calc date.

        Args:
            isins: List of ISINs for which key figures want to be retrieved.
            keyfigures: List of bond key figures which should be retrieved.
                Can be a list of BondKeyFigureName or string.
            calc_date: Date of calculation.
            as_df: if True, the results are represented as pd.DataFrame,
                else as dictionary.

        Returns:
            dictionary containing requested data. If as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return BondKeyFigures(self._client, isins, keyfigures, calc_date).to_df()
        else:
            return BondKeyFigures(self._client, isins, keyfigures, calc_date).to_dict()

    def get_index_composition(
        self, indices: List[str], calc_date: datetime, as_df: bool = False
    ) -> Any:
        """Retrieves index composition for a given set og indices and calc date.

        Args:
            indices: List of Indices for which the
                index composition should be retrieved.
            calc_date: Date of calculation.
            as_df: if True, the results are represented
                as pd.DataFrame, else as dictionary.

        Returns:
            dict containing requested data. if as_df is True,
                the data is in form of a DataFrame.
        """
        if as_df:
            return IndexComposition(self._client, indices, calc_date).to_df()
        else:
            return IndexComposition(self._client, indices, calc_date).to_dict()

    def get_time_series(
        self,
        symbol: List[str],
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
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
                as pd.DataFrame, else as dictionary.

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
        curve: str,
        from_date: datetime,
        to_date: datetime,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        tenors: List[float],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float = None,
        as_df: bool = False,
    ) -> Any:
        """Retrieves historical time series of curve points for a given tenor.

        Args:
            curve: Name of curve that should be retrieved.
            from_date: The first date showing historical curve point.
            to_date: The last date showing historical curve point.
            curve_type:What type of curve is retrieved.
            time_convention: Time convention used when curve is constructed.
            tenors: For what tenors should be curve be constructed.
            spot_forward: Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Forward tenor for forward or implied forward curve.
            as_df: if True, the results are represented
                as pd.DataFrame, else as dictionary.


        Returns:
            dict containing requested data. if as_df is True,
                the data is in form of a DataFrame
        """
        if as_df:
            return CurveTimeSeries(
                self._client,
                curve,
                from_date,
                to_date,
                curve_type,
                time_convention,
                tenors,
                spot_forward,
                forward_tenor,
            ).to_df()
        else:
            return CurveTimeSeries(
                self._client,
                curve,
                from_date,
                to_date,
                curve_type,
                time_convention,
                tenors,
                spot_forward,
                forward_tenor,
            ).to_dict()
