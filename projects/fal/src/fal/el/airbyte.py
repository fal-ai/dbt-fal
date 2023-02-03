"""Synchronize with Airbyte."""
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import requests
import time
from requests.exceptions import RequestException
from faldbt.logger import LOGGER

from fal.telemetry import telemetry


class AirbyteJobState:
    """Possibe Airbyte job states."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    PENDING = "pending"
    FAILED = "failed"
    ERROR = "error"
    INCOMPLETE = "incomplete"


@dataclass
class AirbyteClient:
    """Airbyte REST API connector class."""

    host: str
    max_retries: int = 5
    retry_delay: float = 5
    _base_url: str = field(init=False)

    def __post_init__(self):
        """Set variables."""
        self._base_url = self.host + "/api/v1"

    def request(self, endpoint: str, data: Optional[Dict[str, Any]]):
        """Make a request to Airbyte REST API endpoint."""
        headers = {"accept": "application/json"}

        num_retries = 0
        while True:
            try:
                response = requests.request(
                    method="POST",
                    url=self._base_url + endpoint,
                    headers=headers,
                    json=data,
                    timeout=5,
                )
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                LOGGER.warn(f"Airbyte API request failed: {e}")
                if num_retries == self.max_retries:
                    break
                num_retries += 1
                time.sleep(self.retry_delay)

        raise Exception("Exceeded max number of retries.")

    @telemetry.log_call("airbyte_client_sync")
    def sync(self, connection_id: str) -> dict:
        """Start Airbyte connection sync."""
        return self.request(
            endpoint="/connections/sync", data={"connectionId": connection_id}
        )

    def get_job_status(self, job_id: int) -> dict:
        """Get job status."""
        return self.request(endpoint="/jobs/get", data={"id": job_id})

    def get_connection_data(self, connection_id: str) -> dict:
        """Get details of a connection."""
        return self.request(
            endpoint="/connections/get", data={"connectionId": connection_id}
        )

    @telemetry.log_call("airbyte_client_sync_and_wait")
    def sync_and_wait(
        self,
        connection_id: str,
        interval: float = 10,
        timeout: float = None,
    ):
        """Start a sync operation for the given connector, and polls until done."""
        connection = self.get_connection_data(connection_id)
        job = self.sync(connection_id)
        job_id = job.get("job", {}).get("id")
        LOGGER.info(f"Job {job_id} started for connection: {connection_id}.")
        start = time.monotonic()

        while True:
            if timeout and start + timeout < time.monotonic():
                raise Exception(
                    f"Timeout: Job {job_id} is not finished after {timeout} seconds"
                )

            time.sleep(interval)

            job_details = self.get_job_status(job_id)

            state = job_details.get("job", {}).get("status")

            if state in (
                AirbyteJobState.RUNNING,
                AirbyteJobState.PENDING,
                AirbyteJobState.INCOMPLETE,
            ):
                continue
            elif state == AirbyteJobState.SUCCEEDED:
                break
            elif state == AirbyteJobState.ERROR:
                raise Exception(f"Job failed: {job_id}")
            elif state == AirbyteJobState.CANCELLED:
                raise Exception(f"Job cancelled: {job_id}")
            else:
                raise Exception(f"Unexpected job state `{state}` for job {job_id}")

        return {"job_details": job_details, "connection_details": connection}


@telemetry.log_call("airbyte_sync")
def airbyte_sync(
    host: str,
    connection_id: str,
    interval: float = 10,
    timeout: float = None,
    max_retries: int = 10,
):
    """Sync Airbyte connection."""
    api = AirbyteClient(host=host, max_retries=max_retries)

    return api.sync_and_wait(connection_id, interval=interval, timeout=timeout)
