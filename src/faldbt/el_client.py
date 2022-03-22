from dataclasses import dataclass
from enum import Enum
from fal.el.airbyte import AirbyteClient
from fal.el.fivetran import FivetranClient
from typing import Dict


class ELConfigTypes(Enum):
    AIRBYTE = 1
    FIVETRAN = 2


CONNECTION_KEYS = {"AIRBYTE": "connections", "FIVETRAN": "connectors"}


@dataclass
class FalElClient:
    configs: Dict[str, Dict]

    def airbyte_sync(
        self,
        config_name: str,
        connection_id: str = None,
        connection_name: str = None,
        poll_interval: float = 10,
        poll_timeout: float = None,
        max_retries: int = 10,
    ) -> Dict[str, Dict]:
        return self._run_el_sync(
            config_name=config_name,
            connection_id=connection_id,
            connection_name=connection_name,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )

    def fivetran_sync(
        self,
        config_name: str,
        connector_id: str = None,
        connector_name: str = None,
        poll_interval: float = 10,
        poll_timeout: float = None,
    ) -> Dict[str, Dict]:
        return self._run_el_sync(
            config_name=config_name,
            connection_id=connector_id,
            connection_name=connector_name,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
        )

    def _run_el_sync(
        self,
        config_name: str,
        connection_id: str,
        connection_name: str,
        poll_interval: float = 10,
        poll_timeout: float = None,
    ) -> Dict[str, Dict]:

        el_config = self.configs.get(config_name, None)

        if el_config is None:
            raise Exception(f"EL configuration {config_name} is not found.")

        if connection_id is None and connection_name is None:
            raise Exception(
                "Either connection id or connection name have to be provided."
            )

        config_type = el_config.get("type", "").upper()

        if config_type not in [el.name for el in ELConfigTypes]:
            raise Exception(f"EL configuration type {config_type} is not supported.")

        connection_key = CONNECTION_KEYS[config_type]

        if connection_id is None:
            connections = el_config[connection_key]
            connection = next(
                (c for c in connections if c["name"] == connection_name), None
            )
            if connection is None:
                raise Exception(f"Connection {connection_name} not found.")
            connection_id = connection["id"]

        if config_type == ELConfigTypes.AIRBYTE.name:
            client = AirbyteClient(host=el_config["host"])
            return client.sync_and_wait(connection_id)

        elif config_type == ELConfigTypes.FIVETRAN.name:
            client = FivetranClient(
                api_key=el_config["api_key"], api_secret=el_config["api_secret"]
            )
            return client.sync_and_wait(connector_id=connection_id)

        raise Exception("Not implemented")
