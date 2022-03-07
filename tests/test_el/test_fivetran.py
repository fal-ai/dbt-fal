from mock import patch
from unittest.mock import ANY
import requests

from fal.el.fivetran import FivetranClient


def test_fivetranapi():
    api_client = FivetranClient(api_key="test_key", api_secret="test_secret")
    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        try:
            api_client.get_connector_data(connector_id="test_id")
        except Exception as e:
            mock_request.assert_called_with(
                method="GET",
                url="https://api.fivetran.com/test_id",
                headers={"accept": "application/json"},
                auth=ANY,
                data={},
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."
