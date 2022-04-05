import argparse
from pathlib import Path
from typing import Any, Dict, List
import os

import dbt.exceptions
import dbt.ui
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import raise_for_run_results_failures, run_scripts
from fal.fal_script import FalScript
from faldbt.project import (
    DbtModel,
    FalDbt,
    FalGeneralException,
    FalProject,
    normalize_path,
)


def create_fal_dbt(args: argparse.Namespace):
    real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
    real_profiles_dir = None
    env_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    if args.profiles_dir is not None:
        real_profiles_dir = os.path.realpath(os.path.normpath(args.profiles_dir))
    elif env_profiles_dir:
        real_profiles_dir = os.path.realpath(os.path.normpath(env_profiles_dir))
    else:
        real_profiles_dir = DEFAULT_PROFILES_DIR

    if hasattr(args, "state") and args.state is not None:
        real_state = Path(os.path.realpath(os.path.normpath(args.state)))
    else:
        real_state = None

    return FalDbt(
        real_project_dir,
        real_profiles_dir,
        args.select,
        args.exclude,
        args.selector,
        args.keyword,
        args.threads,
        real_state,
    )


def fal_run(
    args: argparse.Namespace,
    selects_count=0,  # TODO: remove `action="extend"` to match exactly what dbt does
    exclude_count=0,
    script_count=0,
):
    "Runs the fal run command in a subprocess"

    selector_flags = args.select or args.exclude or args.selector
    if args.all and selector_flags:
        raise FalGeneralException(
            "Cannot pass --all flag alongside selection flags (--select/--models, --exclude, --selector)"
        )

    faldbt = create_fal_dbt(args)
    project = FalProject(faldbt)
    models = project.get_filtered_models(args.all, selector_flags, args.before)

    _handle_selector_warnings(selects_count, exclude_count, script_count, args)

    scripts = _select_scripts(args, models)

    if args.before:
        if _should_run_global_scripts(args):
            _run_global_scripts(project, faldbt, "before" if args.before else "after")

        results = run_scripts(scripts, project)
        raise_for_run_results_failures(scripts, results)

    else:
        results = run_scripts(scripts, project)
        raise_for_run_results_failures(scripts, results)

        if _should_run_global_scripts(args):
            _run_global_scripts(project, faldbt, "before" if args.before else "after")


def _handle_selector_warnings(selects_count, exclude_count, script_count, args):
    # TODO: remove `action="extend"` to match exactly what dbt does
    if selects_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --select/--model flags to fal is deprecated and will be removed in fal version 0.4.\n"
            + f"Please use model selection like dbt. Use: --select {' '.join(args.select)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )
    if exclude_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --select/--model flags to fal is deprecated and will be removed in fal version 0.4.\n"
            + f"Please use model exclusion like dbt. Use: --exclude {' '.join(args.exclude)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )
    if script_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --select/--model flags to fal is deprecated and will be removed in fal version 0.4.\n"
            + f"Please use: --script {' '.join(args.scripts)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )


def _should_run_global_scripts(args: argparse.Namespace) -> bool:
    return not bool(args.scripts)


def _select_scripts(
    args: argparse.Namespace, models: List[DbtModel]
) -> List[FalScript]:
    args_dict = vars(args)
    scripts = []
    real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
    scripts_flag = bool(args_dict.get("scripts"))

    for model in models:
        model_scripts = model.get_scripts(args.keyword, bool(args_dict.get("before")))
        for path in model_scripts:
            normalized = normalize_path(real_project_dir, path)
            if not scripts_flag:
                # run all scripts when no --script is passed
                scripts.append(FalScript(model, normalized))
            elif path in args.scripts:
                # if --script selector is there only run selected scripts
                scripts.append(FalScript(model, normalized))

    return scripts


def _run_global_scripts(project: FalProject, faldbt: FalDbt, global_key: str):
    global_scripts = list(
        map(
            lambda path: FalScript(None, Path(path)),
            faldbt._global_script_paths[global_key],
        )
    )

    results = run_scripts(global_scripts, project)
    raise_for_run_results_failures(global_scripts, results)
