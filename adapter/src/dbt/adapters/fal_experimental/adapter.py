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
    get_default_pip_dependencies, _find_adapter_extras
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
        assert not fal_scripts_path.exists()
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
) -> AdapterResponse:
    from isolate.backends.remote import IsolateServer

    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""

    if type(environment).__name__ in ['IsolateServer', 'FalHostedServer']:
        deps = [i for i in get_default_pip_dependencies() if i.startswith('dbt-')]

        if not any('dbt-fal' in i for i in deps):
            # HACK: hard-coding dbt-fal version to install in remote environment since local version could not be found
            # TODO: improve dbt-fal version resolution
            dbt_fal_dep = "dbt-fal"
            dbt_fal_extras = _find_adapter_extras("dbt-fal")
            if dbt_fal_extras:
                dbt_fal_dep += f"[{' ,'.join(dbt_fal_extras)}]"

            deps.append(f"{dbt_fal_dep}==1.3.7")

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
        deps = get_default_pip_dependencies()
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
