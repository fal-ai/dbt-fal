from __future__ import annotations

from functools import partial
from typing import Any

from dbt.adapters.base.impl import BaseAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse

from isolate.backends.virtualenv import PythonIPC, VirtualPythonEnvironment
from dbt.adapters.fal_experimental.utils.environments import get_default_pip_dependencies
from dbt.parser.manifest import MacroManifest, Manifest

from isolate.backends import BaseEnvironment

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    write_df_to_relation,
)

from .utils import retrieve_symbol

def run_with_adapter(code: str, adapter: BaseAdapter) -> Any:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    main = retrieve_symbol(code, "main")
    return main(
        read_df=prepare_for_adapter(adapter, read_relation_as_df),
        write_df=prepare_for_adapter(adapter, write_df_to_relation),
    )

def _isolated_runner(code: str, config: RuntimeConfig, manifest: Manifest, macro_manifest: MacroManifest) -> Any:
    # This function can be run in an entirely separate
    # process or an environment, so we need to reconstruct
    # the DB adapter solely from the config.
    adapter = reconstruct_adapter(config, manifest, macro_manifest)
    return run_with_adapter(code, adapter)

def run_in_environment_with_adapter(
    environment: BaseEnvironment,
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest
) -> AdapterResponse:
    from isolate.backends.remote import IsolateServer
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""
    if type(environment) == IsolateServer:
        deps = [i for i in get_default_pip_dependencies() if i.startswith('dbt-')]

        if environment.target_environment_kind == 'conda':
            raise NotImplementedError("Remote environment with `conda` is not supported yet.")
        else:
            environment.target_environment_config['requirements'].extend(deps)

        key = environment.create()

        with environment.open_connection(key) as connection:
            execute_model = partial(_isolated_runner, code, config, manifest, macro_manifest)
            result = connection.run(execute_model)
            return result


    else:
        deps = get_default_pip_dependencies()
        stage = VirtualPythonEnvironment(deps)

        with PythonIPC(environment, environment.create(), extra_inheritance_paths=[stage.create()]) as connection:
            execute_model = partial(_isolated_runner, code, config, manifest, macro_manifest)
            result = connection.run(execute_model)
            return result
