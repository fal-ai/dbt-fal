from mock import patch
import requests

from fal.sync.airbyte import AirbyteAPI, airbyte_sync


def test_airbyte_sync_request():
    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()

        try:
            airbyte_sync(
                host="test_host",
                connection_id="test_id",
                max_retries=0)

        except Exception as e:
            mock_request.assert_called_with(
                method='POST',
                url='test_host/api/v1/connections/get',
                headers={'accept': 'application/json'},
                json={'connectionId': 'test_id'},
                timeout=5)
            assert str(e) == "Exceeded max number of retries."


def test_airbyteapi():
    api_client = AirbyteAPI(host='test_host', max_retries=0)

    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        try:
            api_client.get_connection_data(connection_id="test_id")

        except Exception as e:
            mock_request.assert_called_with(
                method='POST',
                url='test_host/api/v1/connections/get',
                headers={'accept': 'application/json'},
                json={'connectionId': 'test_id'},
                timeout=5)
            assert str(e) == "Exceeded max number of retries."

    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        try:
            api_client.get_job_status(job_id="test_job_id")

        except Exception:
            mock_request.assert_called_with(
                method='POST',
                url='test_host/api/v1/jobs/get',
                headers={'accept': 'application/json'},
                json={'id': 'test_job_id'},
                timeout=5)

    with patch("requests.request") as mock_request:
        mock_request.side_effect = requests.exceptions.ConnectionError()
        try:
            api_client.sync(connection_id="test_id")

        except Exception:
            mock_request.assert_called_with(
                method='POST',
                url='test_host/api/v1/connections/sync',
                headers={'accept': 'application/json'},
                json={'connectionId': 'test_id'},
                timeout=5)
