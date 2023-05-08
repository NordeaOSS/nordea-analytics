from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd

from nordea_analytics.instrument_variable_names import (
    BenchmarkName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import CustomWarningCheck
from nordea_analytics.nalib.util import (
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BenchmarkDefinition(ValueRetriever):
    """Retrieves and reformats calculated bond key figure.

    Args:
        client: The client used to retrieve data.
        benchmarks: ISIN or name of benchmarks to retrieve definition for.

    Attributes:
        benchmarks: List of benchmark names to retrieve definition for.
        _data: Dictionary to store retrieved benchmark definitions in the format {benchmark_name: benchmark_data}.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        benchmarks: Union[
            str,
            BenchmarkName,
            list[str],
            list[BenchmarkName],
            List[Union[str, BenchmarkName]],
        ],
    ) -> None:
        """Initialization of the class.

        Args:
            client: An instance of DataRetrievalServiceClient or DataRetrievalServiceClientTest for testing.
            benchmarks: ISIN or name of benchmarks to retrieve definition for.
        """
        super(BenchmarkDefinition, self).__init__(client)
        self._client = client

        _benchmarks: List = benchmarks if isinstance(benchmarks, list) else [benchmarks]
        self.benchmarks = [
            convert_to_variable_string(benchmark, BenchmarkName)
            if isinstance(benchmark, BenchmarkName)
            else benchmark
            for benchmark in _benchmarks
        ]

        result = self.get_benchmark_definition()
        self._data = self.format_benchmark_names(result, _benchmarks)

    def format_benchmark_names(
        self,
        data: Dict,
        benchmarks: Union[
            List[str], List[BenchmarkName], List[Union[str, BenchmarkName]]
        ],
    ) -> Dict:
        """Formats benchmark names to be similar to input.

        Args:
            data: Dictionary containing retrieved benchmark definitions.
            benchmarks: List of benchmark names to retrieve definition for.

        Returns:
            Dictionary containing formatted benchmark names.
        """
        for benchmark in benchmarks:
            benchmark_string: Union[str, ValueError]
            if isinstance(benchmark, BenchmarkName):
                benchmark_string = convert_to_variable_string(
                    benchmark, BenchmarkName
                ).upper()
                if benchmark_string in data:
                    # Update dictionary key to match benchmark name
                    data[benchmark.name] = data.pop(benchmark_string)
                    # Update "Benchmark" key in underlying bond info to match benchmark name
                    for underlying_bond_info in data[benchmark.name]:
                        underlying_bond_info["Benchmark"] = benchmark.name

        return data

    def get_benchmark_definition(self) -> Dict:
        """Retrieves response with benchmark definition.

        Returns:
            Dictionary containing retrieved benchmark definitions.
        """
        json_response = self.get_post_get_response()

        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieves response after posting the request.

        Returns:
            Parsed response as a dictionary.
        """
        json_response: Dict = {}
        for request_dict in self.request:
            try:
                _json_response = self._client.get_response(
                    request_dict, self.url_suffix
                )
                # Extract underlying bond information from the response
                _json_response = _json_response[
                    config["results"]["benchmark_definition"]
                ]
                _benchmark_list = []
                for underlyingBond in _json_response["underlyings"]:
                    _underlying_dict = {}
                    _underlying_dict["Benchmark"] = _json_response["benchmark"]
                    _underlying_dict["Symbol"] = underlyingBond["symbol"]
                    _underlying_dict["ISIN"] = underlyingBond["isin"]
                    _underlying_dict["ValidFrom"] = datetime.strptime(
                        underlyingBond["valid_from"], "%Y-%m-%d"
                    )
                    _underlying_dict["Country"] = _json_response["country"]
                    _underlying_dict["Tenor"] = _json_response["tenor"]
                    _benchmark_list.append(_underlying_dict)

                json_response[request_dict["Benchmark"]] = _benchmark_list
            except Exception as ex:
                CustomWarningCheck.post_response_not_retrieved_warning(
                    ex, request_dict["Benchmark"]
                )

        return json_response

    @property
    def url_suffix(self) -> str:
        """URL suffix for a given method.

        Returns:
            URL suffix.
        """
        return config["url_suffix"]["benchmark_definition"]

    @property
    def request(self) -> List[Dict]:
        """Get the request dictionaries for each benchmark.

        Returns:
            List of request dictionaries.
        """
        request_dict = []
        for x in range(len(self.benchmarks)):
            initial_request = {
                "Benchmark": self.benchmarks[x],
            }
            request = {
                key: initial_request[key]
                for key in initial_request.keys()
                if initial_request[key] is not None
            }
            request_dict.append(request)  # Append the request dictionary to the list

        return request_dict

    def to_dict(self) -> Dict:
        """Convert the json response to a dictionary.

        Returns:
            Reformatted response as a dictionary.
        """
        _dict: Dict[str, Any] = {}
        for benchmark in self._data:
            _dict[benchmark] = self._data[benchmark]  # Copy data to the dictionary
        return _dict

    def to_df(self) -> pd.DataFrame:
        """Convert the json response to a pandas DataFrame.

        Returns:
            Reformatted response as a DataFrame.
        """
        _dict = self.to_dict()  # Get the dictionary representation of the response
        df = pd.DataFrame()  # Create an empty DataFrame
        for benchmark in _dict:
            _df = pd.DataFrame.from_dict(
                _dict[benchmark]
            )  # Convert dictionary to DataFrame
            _df = _df.set_index("Benchmark")  # Set 'Benchmark' as the index
            df = pd.concat([df, _df], axis=0)  # Concatenate the DataFrames along rows
        return df
