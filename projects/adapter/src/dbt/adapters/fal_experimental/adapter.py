from __future__ import annotations

import zipfile
import io
from functools import partial
from tempfile import NamedTemporaryFile
from typing import Any, Optional, cast

from dbt.adapters.base.impl import BaseAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse

from koldstart import KoldstartHost, isolated
from dbt.adapters.fal_experimental.utils.environments import (
    EnvironmentDefinition,
    get_default_pip_dependencies
)

from dbt.parser.manifest import MacroManifest, Manifest

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
        if fal_scripts_path.exists():
            import shutil
            shutil.rmtree(fal_scripts_path)
        fal_scripts_path.parent.mkdir(parents=True, exist_ok=True)
        zip_file = zipfile.ZipFile(io.BytesIO(local_packages))
        zip_file.extractall(fal_scripts_path)

    return run_with_adapter(code, adapter, config)


def run_in_environment_with_adapter(
    environment: EnvironmentDefinition,
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
    compressed_local_packages = None

    deps = get_default_pip_dependencies(
        is_remote=(type(environment.host)==KoldstartHost),
        adapter_type=adapter_type)

    fal_scripts_path = get_fal_scripts_path(config)

    if fal_scripts_path.exists():
        with NamedTemporaryFile() as temp_file:
            with zipfile.ZipFile(
                temp_file.name, "w", zipfile.ZIP_DEFLATED
            ) as zip_file:
                for entry in fal_scripts_path.rglob("*"):
                    zip_file.write(entry, entry.relative_to(fal_scripts_path))

            compressed_local_packages = temp_file.read()

    execute_model = partial(
        _isolated_runner,
        code,
        config,
        manifest,
        macro_manifest,
        local_packages=compressed_local_packages
    )

    if environment.kind == "virtualenv":
        requirements = environment.config.get("requirements", [])
        requirements += deps

        isolated_function = isolated(
            kind="virtualenv",
            host=environment.host,
            **environment.config
        )(execute_model)
    # elif environment.kind == "conda":
    #     env_dict = {
    #         "name": "dbt_fal_env",
    #         "channels": ["conda-forge", "defaults"],
    #         "dependencies": [env]
    #     }
    #     for env in environment.target_environments:
    #         if env.get("configuration", {}).get("packages"):
    #             env_dict["dependencies"] += env["configuration"]["packages"]
    #         if env.get("configuration", {}).get("requirements"):
    #             env_dict["dependencies"].append({"pip": env["configuration"]["requirements"]})
    #     isolated_function = isolated(
    #         kind="conda",
    #         host=host,
    #         env_dict=env_dict,
    #         machine_type=environment.machine_type)(execute_model)
    else:
        # We should not reach this point, because environment types are validated when the
        # environment objects are created (in utils/environments.py).
        raise Exception(f"Environment type not supported: {environment.kind}")

    result = isolated_function()
    return result
