from datetime import datetime
from typing import List, Union

import pandas as pd
import pytest

from nordea_analytics import (
    CurveName,
    CurveType,
    NordeaAnalyticsService,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.nalib.exceptions import AnalyticsResponseError


@pytest.fixture
def anchor() -> datetime:
    """Value date for tests."""
    return datetime(2022, 8, 1)


class TestGetCurve:
    """Test class for get curve."""

    @pytest.mark.parametrize(
        "curves, curve_type, tenor_frequency, time_convention, spot_forward, forward_tenor",
        [
            (
                CurveName.DKKMTGNYK,
                CurveType.NelsonSiegel,
                0.5,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
        ],
    )
    def test_get_curve_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        curve_type: Union[str, CurveType],
        tenor_frequency: float,
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if dictionary results are correct."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type,
            tenor_frequency=tenor_frequency,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, curve_type, tenor_frequency, time_convention, spot_forward, forward_tenor",
        [
            (
                [CurveName.DKKMTGNYK, "GRDGOV", CurveName.EURSWAP_Disc_OIS],
                CurveType.NelsonSiegel,
                0.5,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
        ],
    )
    def test_get_curve_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        curve_type: Union[str, CurveType],
        tenor_frequency: float,
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if DataFrame results are correct."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type,
            tenor_frequency=tenor_frequency,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
            as_df=True,
        )

        curve_names = pd.DataFrame(curve_results).index

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_names

    @pytest.mark.parametrize(
        "curves",
        [
            (CurveName.DKKSWAP_Disc_OIS,),
            ("DKKSWAP DISC OIS",),
            ([CurveName.DKKSWAP_Disc_OIS, CurveName.EURSWAP_Disc_OIS],),
            (["DKKSWAP DISC oiS", "EURSWAP DISC OIS"],),
            ([CurveName.DKKSWAP_Disc_OIS, "EURSWAP DISC OIS"],),
        ],
    )
    def test_get_curve_curves_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
    ) -> None:
        """Check if curves input types work as expected."""
        # When only defining a single input, it becomes a tuple...
        curves_input = curves[0] if isinstance(curves, tuple) else curves

        curve_results = na_service.get_curve(
            curves=curves_input,
            calc_date=anchor,
        )

        if curves_input is not None:
            _curves: list = (
                curves_input if isinstance(curves_input, list) else [curves_input]
            )
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, curve_type",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                CurveType.NelsonSiegel,
            ),
            (
                CurveName.DKKSWAP_Disc_OIS,
                "nelsonsiegel",
            ),
        ],
    )
    def test_get_curve_curve_type_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        curve_type: Union[str, CurveType],
    ) -> None:
        """Check if curve_type input types work as expected."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, time_convention",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                TimeConvention.Act365,
            ),
            (
                CurveName.DKKSWAP_Disc_OIS,
                "act365",
            ),
        ],
    )
    def test_get_curve_time_convention_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        time_convention: Union[str, TimeConvention],
    ) -> None:
        """Check if time_convention input types work as expected."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            time_convention=time_convention,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, spot_forward, forward_tenor",
        [
            (
                CurveName.DKKSWAP_Disc_OIS,
                SpotForward.Forward,
                1.0,
            ),
            (
                CurveName.DKKSWAP_Disc_OIS,
                "forward",
                1.0,
            ),
        ],
    )
    def test_get_curve_spot_forward_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if spot_forward input types work as expected."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, curve_type, tenor_frequency, time_convention, spot_forward, forward_tenor",
        [
            (
                [
                    CurveName.DKKMTGNYK
                ],  # should accept enum value despite enum and string value being different
                CurveType.NelsonSiegel,
                0.5,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
            (
                [
                    "DKKMTGNYKSOFTBLT"
                ],  # should accept string value despite enum and string value being different
                CurveType.NelsonSiegel,
                0.5,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
            (
                ["DKKSWAP DISC ois"],  # should be case insensitive of curve names
                CurveType.NelsonSiegel,
                0.5,
                TimeConvention.Act365,
                SpotForward.Forward,
                1.0,
            ),
        ],
    )
    def test_get_curve_curves_input_output_match(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        curve_type: Union[str, CurveType],
        tenor_frequency: float,
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if curves input and output match or correspond to Enum.name value."""
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type,
            tenor_frequency=tenor_frequency,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curves, curve_type, tenor_frequency, time_convention, spot_forward, forward_tenor",
        [
            (
                [CurveName.DKKSWAP_Disc_OIS],  # should be case insensitive
                "nelsonsiegel",
                0.5,
                "act365",
                "forward",
                1.0,
            ),
        ],
    )
    def test_get_curve_curve_case_insensitivity(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        curve_type: Union[str, CurveType],
        tenor_frequency: float,
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: float,
    ) -> None:
        """Check if inputs are case insensitive by comparing result between enums and capitalised string inputs."""
        # Should probably be tested on API side instead
        curve_results = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type,
            tenor_frequency=tenor_frequency,
            time_convention=time_convention,
            spot_forward=spot_forward,
            forward_tenor=forward_tenor,
        )

        curve_results_upper = na_service.get_curve(
            curves=curves,
            calc_date=anchor,
            curve_type=curve_type.name.upper()
            if isinstance(curve_type, CurveType)
            else curve_type.upper(),
            tenor_frequency=tenor_frequency,
            time_convention=time_convention.name.upper()
            if isinstance(time_convention, TimeConvention)
            else time_convention.upper(),
            spot_forward=spot_forward.name.upper()
            if isinstance(spot_forward, SpotForward)
            else spot_forward.upper(),
            forward_tenor=forward_tenor,
        )

        _curves: list = curves if isinstance(curves, list) else [curves]
        for curve in _curves:
            curve_string = (
                CurveName(curve).name if isinstance(curve, CurveName) else curve
            )

            assert curve_string in curve_results
            assert curve_string in curve_results_upper

            expected_values = curve_results[curve_string]["Level"]
            actual_values = curve_results_upper[curve_string]["Level"]
            number_of_points = len(expected_values)
            for i in range(0, number_of_points):
                assert expected_values[i]["Tenor"] == actual_values[i]["Tenor"]
                assert expected_values[i]["Value"] == actual_values[i]["Value"]

    @pytest.mark.parametrize(
        "curves",
        [
            CurveName.DKKSWAP_Disc_OIS,  # should be case insensitive
        ],
    )
    # No data returned
    def test_get_curve_get_result_for_job_id_failed(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
    ) -> None:
        """Check if inputs are case insensitive by comparing result between enums and capitalised string inputs."""
        try:
            curve_results = na_service.get_curve(  # noqa
                curves=curves,
                calc_date=datetime(2008, 8, 1),
            )
        except Exception as exc:
            if isinstance(exc.args[0], AnalyticsResponseError):
                assert "Can't get result for jobId:" in exc.args[0].args[0]
            else:
                raise Exception("AnalyticsResponseError expected") from exc
