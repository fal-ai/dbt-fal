from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, NewType, Optional
from functools import partial
import functools
import zipfile
from dbt.adapters.fal_experimental.connections import TeleportTypeEnum
from dbt.adapters.fal_experimental.utils.environments import EnvironmentDefinition, get_default_pip_dependencies
from dbt.adapters.fal_experimental.utils import extra_path, get_fal_scripts_path, retrieve_symbol
from dbt.config.runtime import RuntimeConfig
from fal_serverless import FalServerlessHost, isolated
import pandas as pd

from dbt.contracts.connection import AdapterResponse

from dbt.fal.adapters.teleport.info import TeleportInfo


DataLocation = NewType('DataLocation', Dict[str, str])

def _prepare_for_teleport(function: Callable, teleport: TeleportInfo, locations: DataLocation) -> Callable:
    @functools.wraps(function)
    def wrapped(relation: str, *args, **kwargs) -> Any:
        relation = relation.lower()
        return function(teleport, locations, relation, *args, **kwargs)

    return wrapped

def _teleport_df_from_external_storage(teleport_info: TeleportInfo, locations: DataLocation, relation: str) -> pd.DataFrame:
    if relation not in locations:
        raise RuntimeError(f"Could not find url for '{relation}' in {locations}")

    if teleport_info.format == 'parquet':
        relation_path = locations[relation]
        url = teleport_info.build_url(relation_path)
        storage_options = _build_teleport_storage_options(teleport_info)
        return pd.read_parquet(url, storage_options=storage_options)
    else:
        # TODO: support more
        raise RuntimeError(f"Format {teleport_info.format} not supported")

def _teleport_df_to_external_storage(teleport_info: TeleportInfo, locations: DataLocation, relation: str, data: pd.DataFrame):
    if teleport_info.format == 'parquet':
        relation_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(relation_path)
        storage_options = _build_teleport_storage_options(teleport_info)

        data.to_parquet(url, storage_options=storage_options)
        locations[relation] = relation_path
        return relation_path
    else:
        raise RuntimeError(f"Format {teleport_info.format} not supported")

def _build_teleport_storage_options(teleport_info: TeleportInfo) -> Dict[str, str]:
    storage_options = {}
    if teleport_info.credentials.type == TeleportTypeEnum.REMOTE_S3:
        storage_options = {
            "key": teleport_info.credentials.s3_access_key_id,
            "secret": teleport_info.credentials.s3_access_key
        }
    elif teleport_info.credentials.type == TeleportTypeEnum.LOCAL:
        pass
    else:
        raise RuntimeError(f"Teleport storage type {teleport_info.credentials.type} not supported")
    return storage_options

def run_with_teleport(
        code: str,
        teleport_info: TeleportInfo,
        locations: DataLocation,
        config: RuntimeConfig,
        local_packages: Optional[bytes]) -> str:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    import io
    fal_scripts_path = str(get_fal_scripts_path(config))
    if local_packages is not None:
        if fal_scripts_path.exists():
            import shutil
            shutil.rmtree(fal_scripts_path)
        fal_scripts_path.parent.mkdir(parents=True, exist_ok=True)
        zip_file = zipfile.ZipFile(io.BytesIO(local_packages))
        zip_file.extractall(fal_scripts_path)

    with extra_path(fal_scripts_path):
        main = retrieve_symbol(code, "main")
        return main(
            read_df=_prepare_for_teleport(_teleport_df_from_external_storage, teleport_info, locations),
            write_df=_prepare_for_teleport(_teleport_df_to_external_storage, teleport_info, locations)
        )

def run_in_environment_with_teleport(
    environment: EnvironmentDefinition,
    code: str,
    teleport_info: TeleportInfo,
    locations: DataLocation,
    config: RuntimeConfig,
    adapter_type: str,
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""
    compressed_local_packages = None
    is_remote = type(environment.host) is FalServerlessHost

    deps = get_default_pip_dependencies(
        is_remote=is_remote,
        adapter_type=adapter_type,
        is_teleport=True,
    )

    fal_scripts_path = get_fal_scripts_path(config)

    if is_remote and fal_scripts_path.exists():
        with NamedTemporaryFile() as temp_file:
            with zipfile.ZipFile(
                temp_file.name, "w", zipfile.ZIP_DEFLATED
            ) as zip_file:
                for entry in fal_scripts_path.rglob("*"):
                    zip_file.write(entry, entry.relative_to(fal_scripts_path))

            compressed_local_packages = temp_file.read()

    execute_model = partial(
        run_with_teleport,
        code,
        teleport_info,
        locations,
        config,
        compressed_local_packages)

    if environment.kind == "virtualenv":
        requirements = environment.config.get("requirements", [])
        requirements += deps
        isolated_function = isolated(
            kind="virtualenv",
            host=environment.host,
            requirements=requirements
        )(execute_model)
    elif environment.kind == "conda":
        dependencies = environment.config.pop("packages", [])
        dependencies.append({"pip": deps})
        env_dict = {
            "name": "dbt_fal_env",
            "channels": ["conda-forge", "defaults"],
            "dependencies": dependencies
        }
        isolated_function = isolated(
            kind="conda",
            host=environment.host,
            env_dict=env_dict)(execute_model)
    else:
        # We should not reach this point, because environment types are validated when the
        # environment objects are created (in utils/environments.py).
        raise Exception(f"Environment type not supported: {environment.kind}")

    # Machine type is only applicable in FalServerlessHost
    if is_remote:
        isolated_function = isolated_function.on(machine_type=environment.machine_type)

    result = isolated_function()
    return result
