from __future__ import annotations

import zipfile
import io
from functools import partial
from tempfile import NamedTemporaryFile
from typing import Any, Optional

from dbt.adapters.base.impl import BaseAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse

from isolate.backends.virtualenv import PythonIPC, VirtualPythonEnvironment
from dbt.adapters.fal_experimental.utils.environments import (
    get_default_pip_dependencies
)

from dbt.parser.manifest import MacroManifest, Manifest

from isolate.backends import BaseEnvironment

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    write_df_to_relation,
)

from .utils import extra_path, get_fal_scripts_path, retrieve_symbol


def run_with_adapter(code: str, adapter: BaseAdapter, config: RuntimeConfig) -> Any:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    fal_scripts_path = str(get_fal_scripts_path(config))
    with extra_path(fal_scripts_path):
        main = retrieve_symbol(code, "main")
        return main(
            read_df=prepare_for_adapter(adapter, read_relation_as_df),
            write_df=prepare_for_adapter(adapter, write_df_to_relation),
        )


def _isolated_runner(
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
    local_packages: Optional[bytes] = None,
) -> Any:
    # This function can be run in an entirely separate
    # process or an environment, so we need to reconstruct
    # the DB adapter solely from the config.
    adapter = reconstruct_adapter(config, manifest, macro_manifest)
    fal_scripts_path = get_fal_scripts_path(config)
    if local_packages is not None:
        # Shoule we overwrite this?
        assert not fal_scripts_path.exists(), f"Path: {fal_scripts_path} already exists in isolate cloud."
        fal_scripts_path.parent.mkdir(parents=True, exist_ok=True)
        zip_file = zipfile.ZipFile(io.BytesIO(local_packages))
        zip_file.extractall(fal_scripts_path)

    return run_with_adapter(code, adapter, config)


def run_in_environment_with_adapter(
    environment: BaseEnvironment,
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
    adapter_type: str
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""
    if type(environment).__name__ in ['IsolateServer', 'FalHostedServer']:
        if type(config.credentials).__name__ == 'BigQueryCredentials' and str(config.credentials.method) == 'service-account':
            raise RuntimeError(
                "BigQuery credential method `service-account` is not supported." + \
                " Please use `service-account-json` instead")
        deps = [
            i
            for i in get_default_pip_dependencies(is_remote=True, adapter_type=adapter_type)
        ]

        extra_config = {
            'kind': 'virtualenv',
            'configuration': { 'requirements': deps }
        }

        environment.target_environments.append(extra_config)

        key = environment.create()

        fal_scripts_path = get_fal_scripts_path(config)

        if fal_scripts_path.exists():
            with NamedTemporaryFile() as temp_file:
                with zipfile.ZipFile(
                    temp_file.name, "w", zipfile.ZIP_DEFLATED
                ) as zip_file:
                    for entry in fal_scripts_path.rglob("*"):
                        zip_file.write(entry, entry.relative_to(fal_scripts_path))

                compressed_local_packages = temp_file.read()
        else:
            compressed_local_packages = None

        with environment.open_connection(key) as connection:
            execute_model = partial(
                _isolated_runner,
                code,
                config,
                manifest,
                macro_manifest,
                local_packages=compressed_local_packages,
            )
            result = connection.run(execute_model)
            return result

    else:
        deps = get_default_pip_dependencies(adapter_type)
        stage = VirtualPythonEnvironment(deps)
        fal_scripts_path = get_fal_scripts_path(config)

        with PythonIPC(
            environment,
            environment.create(),
            extra_inheritance_paths=[fal_scripts_path, stage.create()],
        ) as connection:
            execute_model = partial(
                _isolated_runner, code, config, manifest, macro_manifest
            )
            result = connection.run(execute_model)
            return result
