from mock import patch, Mock
from unittest.mock import ANY
import requests

from fal.el.fivetran import FivetranClient


def test_fivetran_api():
    api_client = FivetranClient(
        api_key="test_key", api_secret="test_secret", disable_schedule_on_trigger=False
    )
    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        try:
            api_client.get_connector_data(connector_id="test_id")
        except Exception as e:
            mock_request.assert_called_with(
                method="GET",
                url="https://api.fivetran.com/v1/connectors/test_id",
                headers={"Content-Type": "application/json;version=2"},
                auth=ANY,
                data=None,
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."

        mock_request.reset_mock()
        try:
            api_client.check_connector("test_id")

        except Exception as e:
            mock_request.assert_called_with(
                method="GET",
                url="https://api.fivetran.com/v1/connectors/test_id",
                headers={"Content-Type": "application/json;version=2"},
                auth=ANY,
                data=None,
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."

        mock_request.reset_mock()
        try:
            api_client.update_connector("test_id", {"test_key": "test_value"})

        except Exception as e:
            mock_request.assert_called_with(
                method="PATCH",
                url="https://api.fivetran.com/v1/connectors/test_id",
                headers={"Content-Type": "application/json;version=2"},
                auth=ANY,
                data='{"test_key": "test_value"}',
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."

        mock_request.reset_mock()
        try:
            api_client.update_schedule_type("test_id", "manual")

        except Exception as e:
            mock_request.assert_called_with(
                method="PATCH",
                url="https://api.fivetran.com/v1/connectors/test_id",
                headers={"Content-Type": "application/json;version=2"},
                auth=ANY,
                data='{"schedule_type": "manual"}',
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."

        mock_request.reset_mock()
        try:
            api_client.check_connector = Mock(return_value=None)
            api_client.start_sync("test_id")

        except Exception as e:
            api_client.check_connector.assert_called_once()
            mock_request.assert_called_with(
                method="POST",
                url="https://api.fivetran.com/v1/connectors/test_id/force",
                headers={"Content-Type": "application/json;version=2"},
                auth=ANY,
                data=None,
                timeout=5,
            )
            assert str(e) == "Exceeded max number of retries."

        api_client.check_connector.reset_mock()
