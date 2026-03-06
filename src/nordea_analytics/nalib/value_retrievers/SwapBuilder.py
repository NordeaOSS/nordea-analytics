from typing import Dict, List, Union

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever
from nordea_analytics.swap_definition import SwapDefinition  # type: ignore[attr-defined]

config = get_config()


class SwapBuilder(ValueRetriever):
    """Build swaps from strings.

    Args:
        swaps: Each swap is defined as a string.
    """

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted JSON data.
        """
        pass

    def __init__(
        self, client: DataRetrievalServiceClient, swaps: Union[str, List[str]]
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            swaps: Each swap is defined as a string.
        """
        super(SwapBuilder, self).__init__(client)
        self._client = client

        self.swap_definitions = swaps if isinstance(swaps, list) else [swaps]

        self._data = self.build_swaps()

    def build_swaps(self) -> Dict:
        """Builds swaps in proper format for swap calculation methods.

        Returns:
            The swaps in dictionary format.
        """
        json_response = self.get_response(self.request)
        return json_response

    def get_response(self, request: Dict) -> Dict:
        """Call the DataRetrievalServiceClient to get a response from the service.

        Args:
            request (Dict): The request dictionary.

        Returns:
            Dict: The response from the service for a given method and request.
        """
        json_response = self._client.get(request, self.url_suffix)
        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            The URL suffix for the swap builder method.
        """
        return config["url_suffix"]["swap_builder"]

    @property
    def request(self) -> Dict:
        """Get request list to build swaps.

        Returns:
            Request dictionary of built swaps.
        """
        request = {
            "swap-definitions": self.swap_definitions,
        }

        return request

    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted JSON data.
        """
        swap_definitions: dict = {}
        swap_list: list[dict] = self._data["swap_definitions"]

        for swap in swap_list:
            swap_definition: SwapDefinition = SwapDefinition(
                currency_paid=swap["currency_paid"],
                currency_received=swap["currency_received"],
                type_paid=swap["type_paid"],
                type_received=swap["type_received"],
                tenor=swap["tenor"],
                start=swap["start"] if "start" in swap else None,
                fix_frequency_paid=(
                    swap["fix_frequency_paid"] if "fix_frequency_paid" in swap else None
                ),
                fix_frequency_received=(
                    swap["fix_frequency_received"]
                    if "fix_frequency_received" in swap
                    else None
                ),
                fixed_rate_paid=(
                    swap["fixed_rate_paid"] if "fixed_rate_paid" in swap else None
                ),
                fixed_rate_received=(
                    swap["fixed_rate_received"]
                    if "fixed_rate_received" in swap
                    else None
                ),
                floating_spread_paid=(
                    swap["floating_spread_paid"]
                    if "floating_spread_paid" in swap
                    else None
                ),
                floating_spread_received=(
                    swap["floating_spread_received"]
                    if "floating_spread_received" in swap
                    else None
                ),
                day_count_convention_paid=(
                    swap["day_count_convention_paid"]
                    if "day_count_convention_paid" in swap
                    else None
                ),
                day_count_convention_received=(
                    swap["day_count_convention_received"]
                    if "day_count_convention_received" in swap
                    else None
                ),
                date_roll_convention=(
                    swap["date_roll_convention"]
                    if "date_roll_convention" in swap
                    else None
                ),
            )

            swap_definitions[swap["name"]] = swap_definition

        return swap_definitions
