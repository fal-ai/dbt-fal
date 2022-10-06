from typing import Any, Callable, Dict, NewType
from functools import partial
import functools
import pandas as pd

from dbt.contracts.connection import AdapterResponse

from dbt.fal.adapters.teleport.info import TeleportInfo

from fal.packages.environments import BaseEnvironment

from .utils import retrieve_symbol


DataLocation = NewType('DataLocation', Dict[str, str])

def _prepare_for_teleport(function: Callable, teleport: TeleportInfo, locations: DataLocation) -> Callable:
    @functools.wraps(function)
    def wrapped(relation: str, *args, **kwargs) -> Any:
        return function(teleport, locations, relation, *args, **kwargs)

    return wrapped

def _teleport_df_from_external_storage(teleport_info: TeleportInfo, locations: DataLocation, relation: str) -> pd.DataFrame:
    if relation not in locations:
        raise RuntimeError(f"Could not find url for '{relation}'")

    if teleport_info.format == 'parquet':
        relation_path = locations[relation]
        url = teleport_info.build_url(relation_path)
        return pd.read_parquet(url)
    else:
        # TODO: support more
        raise RuntimeError(f"Format {teleport_info.format} not supported")

def _teleport_df_to_external_storage(teleport_info: TeleportInfo, locations: DataLocation, relation: str, data: pd.DataFrame):
    if teleport_info.format == 'parquet':
        relation_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(relation_path)
        data.to_parquet(url)
        locations[relation] = relation_path
        return relation_path
    else:
        raise RuntimeError(f"Format {teleport_info.format} not supported")

def run_with_teleport(code: str, teleport_info: TeleportInfo, locations: DataLocation) -> str:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    main = retrieve_symbol(code, "main")
    return main(
        read_df=_prepare_for_teleport(_teleport_df_from_external_storage, teleport_info, locations),
        write_df=_prepare_for_teleport(_teleport_df_to_external_storage, teleport_info, locations)
    )

def run_in_environment_with_teleport(
    environment: BaseEnvironment,
    code: str,
    teleport_info: TeleportInfo,
    locations: DataLocation,
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""

    with environment.connect() as connection:
        execute_model = partial(run_with_teleport, code, teleport_info, locations)
        result = connection.run(execute_model)  # type: ignore
        return result
