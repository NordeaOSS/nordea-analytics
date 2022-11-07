from datetime import datetime
from typing import List, Union
import warnings

from numpy import datetime64
import pandas as pd
import pytest

from nordea_analytics import (
    CurveName,
    CurveType,
    NordeaAnalyticsService,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.nalib.util import float_to_tenor_string


@pytest.fixture
def from_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2021, 1, 4)


@pytest.fixture
def to_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2021, 1, 6)


class TestGetCurveTimeSeries:
    """Test class for get curve time series."""

    @pytest.mark.parametrize(
        "curves, tenors, curve_type, time_convention, spot_forward, forward_tenor",
        [
            (
                CurveName.DKKMTGNYK,
                [0.25, 0.5, 1, 3],
                CurveType.NelsonSiegel,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
        ],
    )
    def test_curve_time_series_dict(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if dictionary results are correct."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
            curve_type=curve_type,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        _tenors = tenors if isinstance(tenors, list) else [tenors]
        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

                expected_number_of_tenors = len(_tenors)
                actual_number_of_tenors = len(list(curve_results[curve_string]))

                assert expected_number_of_tenors == actual_number_of_tenors

                curve_tenor_key = self.create_curve_tenor_key(
                    curve_string, forward_tenor, _tenors[0]
                )
                output_key = curve_results[curve_string][curve_tenor_key]["Date"][0]
                output_value = curve_results[curve_string][curve_tenor_key]["Value"][0]

                assert isinstance(output_key, datetime)
                assert isinstance(output_value, float)

    @pytest.mark.parametrize(
        "curves, tenors, curve_type, time_convention, spot_forward, forward_tenor",
        [
            (
                [CurveName.DKKMTGNYK, "GRDGOV", CurveName.EURSWAP_Disc_OIS],
                [0.25, 0.5, 1, 5],
                CurveType.NelsonSiegel,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
        ],
    )
    def test_curve_time_series_df(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if DataFrame results are correct."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
            curve_type=curve_type,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
            as_df=True,
        )

        _tenors = tenors if isinstance(tenors, list) else [tenors]
        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )

                output_columns = list(pd.DataFrame(curve_results))
                curve_in_output = False
                for x in output_columns:
                    if str(x).__contains__(curve_string):
                        curve_in_output = True

                assert curve_in_output

                dates = curve_results.Date
                assert isinstance(dates.values[0], datetime64)

                curve_tenor_key = self.create_curve_tenor_key(
                    curve_string, forward_tenor, _tenors[0]
                )

                output_value = curve_results[curve_tenor_key].values[0]
                assert isinstance(output_value, float)

    @pytest.mark.parametrize(
        "curves, tenors",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                [0.25, 0.5, 1, 3],
            ),
            (
                "DKKSWAP DISC OIS",
                [0.25, 0.5, 1, 3],
            ),
            (
                [CurveName.DKKSWAP_Disc_OIS, CurveName.EURSWAP_Disc_OIS],
                [0.25, 0.5, 1, 3],
            ),
            (
                ["DKKSWAP DISC oiS", "EURSWAP DISC OIS"],
                [0.25, 0.5, 1, 3],
            ),
            (
                [CurveName.DKKSWAP_Disc_OIS, "EURSWAP DISC OIS"],
                [0.25, 0.5, 1, 3],
            ),
        ],
    )
    def test_curve_time_series_curves_inputs(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
    ) -> None:
        """Check if curves input types work as expected."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
        )

        expected_curve_names = []
        _curves = curves if isinstance(curves, list) else [curves]
        _tenors = tenors if isinstance(tenors, list) else [tenors]
        for curve in _curves:
            for tenor in _tenors:
                curve_string = curve.name if isinstance(curve, CurveName) else curve
                curve_tenor_key = self.create_curve_tenor_key(curve_string, None, tenor)
                expected_curve_names.append(curve_tenor_key)

        actual_curve_names: set[str] = set().union(*[x for x in curve_results.values()])  # type: ignore[assignment]
        assert sorted(expected_curve_names) == sorted(actual_curve_names)

    @pytest.mark.parametrize(
        "curves, tenors, curve_type",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                [0.25, 0.5, 1, 3],
                CurveType.NelsonSiegel,
            ),
            (
                "DKKSWAP DISC OIS",
                [0.25, 0.5, 1, 3],
                "nelsonsiegel",
            ),
        ],
    )
    def test_curve_time_series_curve_type_inputs(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
        curve_type: Union[str, CurveType],
    ) -> None:
        """Check if curve_type input types work as expected."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
            curve_type=curve_type,
        )

        expected_curve_names = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        _tenors = tenors if isinstance(tenors, list) else [tenors]
        for curve in _curves:
            for tenor in _tenors:
                curve_string = curve.name if isinstance(curve, CurveName) else curve
                curve_tenor_key = self.create_curve_tenor_key(curve_string, None, tenor)
                expected_curve_names.append(curve_tenor_key)

        actual_curve_names: set[str] = set().union(*[x for x in curve_results.values()])  # type: ignore[assignment]
        assert sorted(expected_curve_names) == sorted(actual_curve_names)

    @pytest.mark.parametrize(
        "curves, tenors, time_convention",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                [0.25, 0.5, 1, 3],
                TimeConvention.Act365,
            ),
            (
                "DKKSWAP DISC OIS",
                [0.25, 0.5, 1, 3],
                "act365",
            ),
        ],
    )
    def test_curve_time_series_time_convention_inputs(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
        time_convention: Union[str, TimeConvention],
    ) -> None:
        """Check if time_convention input types work as expected."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
            time_convention=time_convention,
        )

        expected_curve_names = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        _tenors = tenors if isinstance(tenors, list) else [tenors]
        for curve in _curves:
            for tenor in _tenors:
                curve_string = curve.name if isinstance(curve, CurveName) else curve
                curve_tenor_key = self.create_curve_tenor_key(curve_string, None, tenor)
                expected_curve_names.append(curve_tenor_key)

        actual_curve_names: set[str] = set().union(*[x for x in curve_results.values()])  # type: ignore[assignment]
        assert sorted(expected_curve_names) == sorted(actual_curve_names)

    @pytest.mark.parametrize(
        "curves, tenors, spot_forward, forward_tenor",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                [0.25, 0.5, 1, 3],
                SpotForward.Forward,
                1.0,
            ),
            (
                "DKKSWAP DISC OIS",
                [0.25, 0.5, 1, 3],
                "Forward",
                1.0,
            ),
        ],
    )
    def test_curve_time_series_spot_forward_inputs(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if spot_forward input types work as expected."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        expected_curve_names = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        _tenors = tenors if isinstance(tenors, list) else [tenors]
        for curve in _curves:
            for tenor in _tenors:
                curve_string = curve.name if isinstance(curve, CurveName) else curve
                curve_tenor_key = self.create_curve_tenor_key(
                    curve_string, forward_tenor, tenor
                )
                expected_curve_names.append(curve_tenor_key)

        actual_curve_names: set[str] = set().union(*[x for x in curve_results.values()])  # type: ignore[assignment]
        assert sorted(expected_curve_names) == sorted(actual_curve_names)

    @pytest.mark.parametrize(
        "curves, tenors",
        [
            (
                # Checking that input and output curve names match with enum different from string value
                [CurveName.DKKMTGNYK],
                [0.25, 0.5, 1, 3],
            ),
            (
                # Checking that input and output curve names match with string definition and odd capitalisation
                ["DKKMTGNYKSOFTBLT", "DKKSWAP DISC ois"],
                [0.25, 0.5, 1, 3],
            ),
        ],
    )
    def test_curve_time_series_curve_time_series_input_output_match(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
    ) -> None:
        """Check if curves input and output match or correspond to Enum.name value."""
        curve_results = na_service.get_curve_time_series(
            curves=curves,
            from_date=from_date,
            to_date=to_date,
            tenors=tenors,
        )

        expected_curve_names = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        _tenors = tenors if isinstance(tenors, list) else [tenors]
        for curve in _curves:
            for tenor in _tenors:
                curve_string = curve.name if isinstance(curve, CurveName) else curve
                curve_tenor_key = self.create_curve_tenor_key(curve_string, None, tenor)
                expected_curve_names.append(curve_tenor_key)

        actual_curve_names: set[str] = set().union(*[x for x in curve_results.values()])  # type: ignore[assignment]
        assert sorted(expected_curve_names) == sorted(actual_curve_names)

    @pytest.mark.parametrize(
        "curves, tenors",
        [([CurveName.DKKGOV, CurveName.DKKSWAP_Disc_OIS], 0.5)],
    )
    def test_curve_time_series_partial_result_and_warning(
        self,
        na_service: NordeaAnalyticsService,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
    ) -> None:
        """Check if curves input and output match or correspond to Enum.name value."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            _curves = curves if isinstance(curves, list) else [curves]
            curve_results = na_service.get_curve_time_series(
                curves=curves,
                from_date=datetime(2008, 10, 1),
                to_date=datetime(2008, 10, 1),
                tenors=tenors,
            )

            if isinstance(w[0].message, Warning):
                warning_message = w[0].message.args[0]
                curve_key = (
                    _curves[0].value
                    if isinstance(_curves[0], CurveName)
                    else _curves[0]
                )
                assert curve_key and "could not be retrieved." in warning_message
                dkkgov_results = curve_results[curve_key]
                curve_tenor_key = f"{curve_key}({tenors}Y)"

                assert len(dkkgov_results[curve_tenor_key]["Value"]) > 0
            else:
                raise Exception("Warning expected")

    @pytest.mark.parametrize(
        "curves, tenors",
        [(CurveName.DKKSWAP_Disc_OIS, 0.5)],
    )
    def test_curve_time_series_no_data_returned(
        self,
        na_service: NordeaAnalyticsService,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        tenors: Union[float, list[float]],
    ) -> None:
        """Check if curves input and output match or correspond to Enum.name value."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            _curves = curves if isinstance(curves, list) else [curves]
            curve_results = na_service.get_curve_time_series(
                curves=curves,
                from_date=datetime(2008, 10, 1),
                to_date=datetime(2008, 10, 1),
                tenors=tenors,
            )

            if isinstance(w[0].message, Warning):
                warning_message = w[0].message.args[0]
                curve_key = (
                    _curves[0].value
                    if isinstance(_curves[0], CurveName)
                    else _curves[0]
                )
                assert curve_key and "could not be retrieved." in warning_message
                assert len(curve_results) == 0
            else:
                raise Exception("Warning expected")

    def create_curve_tenor_key(
        self, curve: str, forward: Union[float, None], tenor: Union[float, None]
    ) -> str:
        """Creates curve tenor key of dict."""
        forward_string: str = ""
        if forward is not None:
            forward_string = "(" + float_to_tenor_string(forward) + ")"
        tenor_string: str = ""
        if tenor is not None:
            tenor_string = "(" + float_to_tenor_string(tenor) + ")"
        curve_tenor_key = curve + forward_string + tenor_string

        return curve_tenor_key
