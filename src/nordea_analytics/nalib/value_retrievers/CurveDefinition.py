from datetime import datetime
from typing import Any, Dict, Mapping, Union

import pandas as pd

from nordea_analytics.curve_variable_names import (
    CurveDefinitionName,
    CurveName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
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
        self.curve = convert_to_variable_string(curve, CurveName)
        self.calc_date = calc_date

        self._data = self.get_curve_definition()

    def get_curve_definition(self) -> Mapping:
        """Retrieves response with curve definition."""
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["curve_definition"]]

        output_found = check_json_response(json_response)
        check_json_response_error(output_found)

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["curve_definition"]

    @property
    def request(self) -> Dict:
        """Request dictionary curve time definition."""
        request = {"date": self.calc_date.strftime("%Y-%m-%d"), "curve": self.curve}

        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        _curve_def_dict: Dict[Any, Any] = {}
        for curve_def in self._data["values"]:
            _curve_def_dict = {}
            _curve_def_dict["Quote"] = convert_to_float_if_float(
                curve_def["asset"]["quote"]
            )
            _curve_def_dict["Weight"] = curve_def["asset"]["weight"]
            _curve_def_dict["Maturity"] = datetime.strptime(
                curve_def["asset"]["maturity"], "%Y-%m-%dT%H:%M:%S.0000000"
            )
            _dict[curve_def["name"]] = _curve_def_dict
        return {self.curve: _dict}

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict[self.curve]).transpose()
        df = df.reset_index().rename(columns={"index": "Name"})
        df.index = [self.curve] * len(df)
        return df
