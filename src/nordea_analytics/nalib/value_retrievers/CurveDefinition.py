from datetime import datetime
from typing import Any, Dict, List, Mapping, Union

import numpy as np
import pandas as pd

from nordea_analytics.curve_variable_names import (
    CurveDefinitionName,
    CurveName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import AnalyticsWarning, CustomWarning
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class CurveDefinition(ValueRetriever):
    """Retrieves and reformat curve definition."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        curve: Union[str, CurveDefinitionName, CurveName],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            curve: Name of curve that should be retrieved.
            calc_date: calculation date for request.
        """
        super(CurveDefinition, self).__init__(client)
        self._client = client
        self.curve: str = (
            convert_to_variable_string(curve, CurveName)
            if type(curve) == CurveName
            else convert_to_variable_string(curve, CurveDefinitionName)
            if type(curve) == CurveDefinitionName
            else str(curve)
        )
        self.curve_original = curve
        self.calc_date = calc_date
        self._data = self.get_curve_definition()

    def get_curve_definition(self) -> Mapping:
        """Retrieves response with curve definition."""
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["curve_definition"]]

        return json_response

    def format_curve_names(
        self,
        data: List,
        curve_name: Union[str, CurveName],
    ) -> List:
        """Formats curve names to be identical to curves input."""
        curve_dict = {}
        curve_name_string: Union[str, ValueError]
        if type(curve_name) == CurveName:
            curve_name_string = convert_to_variable_string(curve_name, CurveName)
            if curve_name_string != ValueError:
                curve_name_string = curve_name_string.upper()
            else:
                CustomWarning(
                    "Conversion of {0} failed.".format(curve_name), AnalyticsWarning
                )
        elif type(curve_name) == str:
            curve_name_string = curve_name.upper()
        curve_dict[curve_name_string] = (
            curve_name.name if type(curve_name) == CurveName else curve_name
        )

        for curve_result in data:
            curve_result["curve"]["curve_specification"]["name"] = curve_dict[
                curve_result["curve"]["curve_specification"]["name"].upper()
            ]

        return data

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["curve_definition"]

    @property
    def request(self) -> Dict:
        """Request dictionary curve time definition."""
        request = {"date": self.calc_date.strftime("%Y-%m-%d"), "curve": self.curve}

        return request

    def get_curve_key(self, curve_name: str) -> str:
        """Get curve key for dict."""
        if curve_name == self.curve_original:  # True when curve is input as string
            curve_key = curve_name
        else:
            try:
                curve_key = CurveName(curve_name).name
            except Exception:
                curve_key = curve_name

        return curve_key

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        _curve_def_dict: Dict[Any, Any] = {}
        for curve_def in self._data["values"]:
            _curve_def_dict = {}
            if "quote" in curve_def["asset"]:
                _curve_def_dict["Quote"] = convert_to_float_if_float(
                    curve_def["asset"]["quote"]
                )
            if "weight" in curve_def["asset"]:
                _curve_def_dict["Weight"] = curve_def["asset"]["weight"]
            if "maturity" in curve_def["asset"]:
                _curve_def_dict["Maturity"] = datetime.strptime(
                    curve_def["asset"]["maturity"], "%Y-%m-%dT%H:%M:%S.0000000"
                )
            curve_key = self.get_curve_key(self.curve)
            _dict[curve_def["name"]] = _curve_def_dict
        return {curve_key: _dict}

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()

        curve_key = (
            self.curve_original.name
            if type(self.curve_original) == CurveName
            or type(self.curve_original) == CurveDefinitionName
            else self.curve_original
        )

        df = pd.DataFrame.from_dict(_dict[curve_key]).transpose()
        df = df.astype(object).mask(df.isna(), np.nan)
        df = df.reset_index().rename(columns={"index": "Name"})
        df.index = [curve_key] * len(df)
        return df
