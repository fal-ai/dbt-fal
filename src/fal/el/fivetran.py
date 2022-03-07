"""Synchronize with Fivetran."""
import time
import requests
import datetime
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from dataclasses import dataclass, field
from typing import Any, Dict, List
from urllib.parse import urljoin
from dateutil import parser
from dbt.logger import GLOBAL_LOGGER as logger

BASE_URL = "https://api.fivetran.com"
CONNECTOR_PATH = "v1/connectors/"


@dataclass
class FivetranClient:
    """Fivetran API client."""

    api_key: str
    api_secret: str
    disable_schedule_on_trigger: bool = True
    max_retries: int = 3
    retry_delay: float = 0.25
    _auth: Any = field(init=False)

    def __post_init__(self):
        """Set variables."""
        self._auth = HTTPBasicAuth(self.api_key, self.api_secret)

    def request(self, endpoint: str, method: str, data: Dict[str, Any] = {}):
        """Make a request to Airbyte REST API endpoint."""
        headers = {"accept": "application/json"}

        num_retries = 0
        while True:
            try:
                response = requests.request(
                    method=method,
                    url=urljoin(BASE_URL, endpoint),
                    headers=headers,
                    auth=self._auth,
                    data=data,
                    timeout=5,
                )
                response.raise_for_status()
                parsed = response.json()
                return parsed["data"] if "data" in parsed else parsed
            except RequestException as e:
                logger.warn("Fivetran API request failed: %s", e)
                if num_retries == self.max_retries:
                    break
                num_retries += 1
                time.sleep(self.retry_delay)

        raise Exception("Exceeded max number of retries.")

    def get_connector_data(self, connector_id: str) -> dict:
        """Get details of a connector."""
        return self.request(method="GET", endpoint=connector_id)

    def check_connector(self, connector_id: str):
        """Check if connector can be synced."""
        connector_data = self.get_connector_data(connector_id)
        if connector_data["status"]["setup_state"] != "connected":
            raise Exception("Cannot sync connector '{connector_id}': not set up.")
        if connector_data["paused"]:
            raise Exception("Cannot sync connector '{connector_id}': paused.")

    def get_sync_status(self, connector_id: str):
        """Return status of the latest sync operation for a given connector."""
        connector_data = self.get_connector_data(connector_id)
        min_time_str = "0001-01-01 00:00:00+00"
        succeeded_at = parser.parse(connector_data["succeeded_at"] or min_time_str)
        failed_at = parser.parse(connector_data["failed_at"] or min_time_str)

        return (
            max(succeeded_at, failed_at),
            succeeded_at > failed_at,
            connector_data["status"]["sync_state"],
        )

    def update_connector(
        self, connector_id: str, properties: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Update connector details."""
        return self.request(method="PATCH", endpoint=connector_id, data=properties)

    def update_schedule_type(
        self, connector_id: str, schedule_type: str = None
    ) -> Dict[str, Any]:
        """Update connector schedule type to "auto" or "manual"."""
        if schedule_type not in ["auto", "manual"]:
            raise Exception("schedule_type must be either 'auto' or 'manual'.")
        return self.update_connector(
            connector_id, properties={"schedule_type": schedule_type}
        )

    def get_connector_schema(self, connector_id: str) -> Dict[str, Any]:
        """Get schema config for a connector."""
        return self.request("GET", endpoint=f"{connector_id}/schemas")

    def start_sync(self, connector_id: str) -> Dict[str, Any]:
        """Start a Fivetran connector sync."""
        if self._disable_schedule_on_trigger:
            logger.info("Disabling Fivetran sync schedule.")
            self.update_schedule_type(connector_id, "manual")
        self._check_connector(connector_id)
        self.make_request(method="POST", endpoint=f"{connector_id}/force")
        connector_data = self.get_connector_data(connector_id)
        logger.info(f"Sync start for connector_id={connector_id}.")
        return connector_data

    def start_resync(
        self, connector_id: str, resync_parameters: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """Start a historical sync of all data for multiple schema tables within a connector."""
        if self._disable_schedule_on_trigger:
            logger.info("Disabling Fivetran sync schedule.")
            self.update_schedule_type(connector_id, "manual")
        self._check_connector(connector_id)
        self.make_request(
            method="POST",
            endpoint=f"{connector_id}/schemas/tables/resync",
            data=resync_parameters,
        )
        connector_data = self.get_connector_data(connector_id)
        logger.info(f"Resync start for connector_id={connector_id}.")
        return connector_data

    def poll_sync(
        self,
        connector_id: str,
        prev_sync_completion: datetime.datetime,
        poll_interval: float = 10,
        poll_timeout: float = None,
    ) -> Dict[str, Any]:
        """Poll Fivetran until the next sync completes."""
        # The previous sync completion time is necessary because the only way to tell when a sync
        # completes is when this value changes.

        poll_start = datetime.datetime.now()
        while True:
            (
                curr_last_sync_completion,
                curr_last_sync_succeeded,
                curr_sync_state,
            ) = self.get_sync_status(connector_id)
            logger.info(f"Connector '{connector_id}'. State: {curr_sync_state}")

            if curr_last_sync_completion > prev_sync_completion:
                break

            if (
                poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=poll_timeout)
            ):
                raise Exception(
                    f"Timed out sync for connector '{connector_id}' after {datetime.datetime.now() - poll_start}."
                )

            # Sleep for set poll interval
            time.sleep(poll_interval)

        connector_data = self.get_connector_data(connector_id)
        if not curr_last_sync_succeeded:
            raise Exception("Sync for connector '{connector_id}' failed!")

        return connector_data

    def sync_and_wait(
        self,
        connector_id: str,
        poll_interval: float = 10,
        poll_timeout: float = None,
    ):
        """Start a sync operation for the given connector, and wait until it completes."""
        schema_config = self.get_connector_schema(connector_id)
        init_last_sync_timestamp, _, _ = self.get_sync_status(connector_id)
        self.start_sync(connector_id)
        final_details = self.poll_sync(
            connector_id,
            init_last_sync_timestamp,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )
        return {"connector_details": final_details, "schema_config": schema_config}

    def resync_and_wait(
        self,
        connector_id: str,
        poll_interval: float = 10,
        poll_timeout: float = None,
    ):
        """Start a sync operation for the given connector, and wait until it completes."""
        schema_config = self.get_connector_schema(connector_id)
        init_last_sync_timestamp, _, _ = self.get_sync_status(connector_id)
        self.start_resync(connector_id)
        final_details = self.poll_sync(
            connector_id,
            init_last_sync_timestamp,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )
        return {"connector_details": final_details, "schema_config": schema_config}
