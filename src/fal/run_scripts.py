"""Run fal scripts."""
from multiprocessing.pool import Pool
import os
from typing import Dict, Any, List, Union
from dataclasses import dataclass
from pathlib import Path

from multiprocessing.dummy import Pool as ThreadPool

from dbt.contracts.results import RunStatus
from dbt.config.runtime import RuntimeConfig
from dbt.logger import GLOBAL_LOGGER as logger

from faldbt.project import DbtModel, FalProject
from fal.fal_script import FalScript
from fal.utils import print_run_info

import faldbt.lib as lib

import traceback

if lib.DBT_VCURRENT.compare(lib.DBT_V1) >= 0:
    from dbt.contracts.graph.parsed import ColumnInfo
else:
    from faldbt.cp.contracts.graph.parsed import ColumnInfo


@dataclass
class CurrentModel:
    name: str
    status: RunStatus
    columns: Dict[str, ColumnInfo]
    tests: List[Any]
    meta: Dict[Any, Any]


@dataclass
class CurrentTest:
    name: str
    modelname: str
    column: str
    status: str


@dataclass
class ContextConfig:
    target_path: Path


@dataclass
class Context:
    current_model: Union[CurrentModel, None]
    config: ContextConfig


def _prepare_exec_script(script: FalScript, project: FalProject) -> bool:
    context = _build_script_context(script, project)
    success: bool = True
    faldbt = project._faldbt

    logger.debug(
        "Running script {} for model {}",
        script.path,
        _get_script_model(script),
    )

    try:
        script.exec(context, faldbt)
    except Exception as err:
        err_str = str.join("\n", traceback.format_exception(err.__class__, err, None))
        logger.error(
            "Error in script {} with model {}:\n{}",
            script.path,
            _get_script_model(script),
            err_str,
        )
        # TODO: what else to do?
        success = False

    finally:
        logger.debug(
            "Finished script {} for model {}",
            script.path,
            _get_script_model(script),
        )

    return success


def run_scripts(scripts: List[FalScript], project: FalProject) -> List[bool]:

    print_run_info(scripts)

    faldbt = project._faldbt

    logger.info("Concurrency: {} threads", faldbt.threads)
    with ThreadPool(faldbt.threads) as pool:
        pool: Pool = pool
        try:
            scripts_with_project = map(lambda script: (script, project), scripts)
            results = pool.starmap(_prepare_exec_script, scripts_with_project)

        except KeyboardInterrupt:
            pool.close()
            pool.terminate()
            pool.join()
            raise

    logger.debug("Script results: {}", results)
    return results


def _build_script_context(script: FalScript, project: FalProject):
    context_config = ContextConfig(_get_target_path(project._faldbt._config))
    if _is_global(script):
        return Context(current_model=None, config=context_config)

    model: DbtModel = script.model  # type: ignore

    meta = model.meta
    _del_key(meta, project.keyword)

    tests = _process_tests(model.tests)

    current_model = CurrentModel(
        name=model.name,
        status=model.status,
        columns=model.columns,
        tests=tests,
        meta=meta,
    )

    return Context(current_model=current_model, config=context_config)


def _is_global(script: FalScript):
    return script.model is None


def _get_script_model(script: FalScript):
    return "<GLOBAL>" if _is_global(script) else script.model.name


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass


def _get_target_path(config: RuntimeConfig) -> Path:
    return Path(os.path.realpath(os.path.join(config.project_root, config.target_path)))


def _process_tests(tests: List[Any]):
    return list(
        map(
            lambda test: CurrentTest(
                name=test.name,
                column=test.column,
                status=test.status,
                modelname=test.model,
            ),
            tests,
        )
    )
