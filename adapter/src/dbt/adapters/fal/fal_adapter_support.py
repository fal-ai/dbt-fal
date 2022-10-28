import pandas as pd
import inspect
from tempfile import NamedTemporaryFile
from pathlib import Path
from dataclasses import dataclass
from functools import cached_property, partial
from dbt.adapters.base import available
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.base.impl import log_code_execution
from multiprocessing import Pool
from dbt.flags import PROFILES_DIR
from fal import FalDbt
import faldbt.lib as lib

FAL_BOILER_PLATE = """
{model_code}

def load_df_function(fal_dbt: FalDbt, relation: str):
    database, schema, identifier = relation.split('.')
    print(database, schema, identifier)

    # it uses config if available
    adapter = lib._get_adapter(None, None, None, config=fal_dbt._config)
    return lib._fetch_relation(fal_dbt.project_dir, fal_dbt.profiles_dir, fal_dbt._profile_target,
        lib._build_table_from_parts(adapter, database, schema, identifier)
    )

# The execution context
dbt_context = dbtObj(partial(load_df_function, fal_dbt))
session = None

# Run the model.
result_df = model(dbt_context, session)
write_to_model(result_df)
"""

def load_df_function(fal_dbt: FalDbt, relation: str):
    database, schema, identifier = relation.split('.')
    print(database, schema, identifier)

    # it uses config if available
    adapter = lib._get_adapter(None, None, None, config=fal_dbt._config)
    return lib._fetch_relation(fal_dbt.project_dir, fal_dbt.profiles_dir, fal_dbt._profile_target,
        lib._build_table_from_parts(adapter, database, schema, identifier)
    )

def run_regular_fal(source_code: str, unique_id: str, project_root: str):
    from fal import FalDbt

    fal_dbt = FalDbt(project_root, PROFILES_DIR)

    # Prepare Python script
    namespace = {}
    exec(source_code, namespace)

    # Execute it
    dbt_context = namespace["dbtObj"](partial(load_df_function, fal_dbt))
    session = None

    result_df = namespace["model"](dbt_context, session)

    parts = namespace["_dbt_get_relation"]
    adapter = lib._get_adapter(None, None, None, config=fal_dbt._config)
    rel = lib._build_table_from_parts(adapter, parts['database'], parts['schema'], parts['identifier'])

    # Apply it to the DB
    adapter_response = lib.overwrite_target(
        result_df,
        fal_dbt.project_dir,
        fal_dbt.profiles_dir,
        fal_dbt._profile_target,
        # TODO: Passing a Relation instead of a CompileResultNode because it may be a non-existent node (temp table)
        # Luckly, we use the same API that Relation has: obj.database, obj.schema, obj.identifier
        rel
    )

    return adapter_response


@dataclass
class _UniqueIdProvider:
    unique_id: str


def run_isolated_fal(
    source_code: str, unique_id: str, project_root: str, environment: str
):
    # Save the formatted source code somewhere in the disk.
    from fal import FalDbt
    from fal.utils import DynamicIndexProvider
    from fal.planner.tasks import FalIsolatedHookTask, HookType, SUCCESS

    # Initialize the real fal-dbt object.
    fal_dbt = FalDbt(project_root, PROFILES_DIR)

    with NamedTemporaryFile(mode="w+t") as tmp_file:
        tmp_file.write(FAL_BOILER_PLATE.format(model_code=source_code))
        tmp_file.flush()

        task = FalIsolatedHookTask(
            hook_path=Path(tmp_file.name),
            bound_model=_UniqueIdProvider(unique_id),
            environment_name=environment,
            hook_type=HookType.MODEL_SCRIPT,
        )
        task.set_run_index(DynamicIndexProvider())
        task_status = task.execute(None, fal_dbt)

    return AdapterResponse("OK" if task_status == SUCCESS else "FAIL")


class FalExecutionMixin:
    @available.parse_none
    @log_code_execution
    def submit_python_job(self, model: dict, compiled_code: str):
        # TODO: We probably should use raw_source_code and do this stuff
        # by ourselves.
        # raw_source_code = model["raw_code"]

        args = (
            compiled_code,
            model["unique_id"],
            self.config.project_root,
        )

        environment = model["meta"].get("fal", {}).get("environment")
        if environment is None:
            func = run_regular_fal
        else:
            func = run_isolated_fal
            args += (environment,)

        with Pool(1) as pool:
            adapter_response = pool.apply(func, args)

        # TODO: Normally fal does this in write_to_model but we disabled it
        # for the time being because when fal is clearing the cache it
        # also re-fills it by generating a new manifest object which doesn't
        # play nicely with our "hack" (generating multiple different config/manifest
        # objects makes our side-effect-ful cause some problems).
        # TODO: I think this breaks models, because it tries to create a table which is already there
        self.cache.clear()
        return adapter_response

    @classmethod
    def type(cls):
        # Will fix this in fal by introducing another property
        # to this class to point out the real adapter.
        for frame in inspect.stack():
            if frame.function == "_alchemy_engine":
                return "postgres"
        return "fal"


class FalCredentialMixin:
    @property
    def type(self) -> str:
        return "fal"
