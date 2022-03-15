import argparse
from pathlib import Path
from typing import Any, Dict, List
import os

import dbt.exceptions
import dbt.ui
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.fal_script import FalScript
from faldbt.project import DbtModel, FalDbt, FalGeneralException, FalProject


def create_fal_dbt(
    args: argparse.Namespace,
):
    real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
    real_profiles_dir = None
    if args.profiles_dir is not None:
        real_profiles_dir = os.path.realpath(os.path.normpath(args.profiles_dir))
    elif os.getenv("DBT_PROFILES_DIR"):
        real_profiles_dir = os.path.realpath(
            os.path.normpath(os.getenv("DBT_PROFILES_DIR"))
        )
    else:
        real_profiles_dir = DEFAULT_PROFILES_DIR

    return FalDbt(
        real_project_dir,
        real_profiles_dir,
        args.select,
        args.exclude,
        args.selector,
        args.keyword,
    )


def fal_run(
    args: argparse.Namespace,
    selects_count=0,  # TODO: remove `action="extend"` to match exactly what dbt does
    exclude_count=0,
    script_count=0,
):
    "Runs the fal run command in a subprocess"

    args_dict = vars(args)
    selector_flags = args.select or args.exclude or args.selector
    if args_dict.get("all") and selector_flags:
        raise FalGeneralException(
            "Cannot pass --all flag alongside selection flags (--select/--models, --exclude, --selector)"
        )

    faldbt = create_fal_dbt(args)
    project = FalProject(faldbt)
    models = project.get_filtered_models(
        args_dict.get("all"), selector_flags, args_dict.get("before")
    )

    _handle_selector_warnings(selects_count, exclude_count, script_count, args)

    scripts = _select_scripts(args, args_dict, models)

    # run model specific scripts first
    run_scripts(scripts, project)

    # then run global scripts
    if _should_run_global_scripts(args_dict):
        _run_global_scripts(
            project, faldbt, "before" if args_dict.get("before") else "after"
        )


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


def _should_run_global_scripts(args_dict) -> bool:
    return args_dict.get("scripts")


def _select_scripts(
    args: argparse.Namespace, args_dict: Dict[str, Any], models: List[DbtModel]
) -> List[FalScript]:
    scripts = []
    # if --script selector is there only run selected scripts
    if args_dict.get("scripts"):
        scripts = []
        for model in models:
            model_scripts = model.get_scripts(args.keyword, args_dict.get("before"))
            for el in args.scripts:
                if el in model_scripts:
                    scripts.append(FalScript(model, Path(el)))
            model_scripts: List[Path] = model.get_scripts(
                args.keyword, bool(args_dict.get("before"))
            )
            args_scripts: List[str] = args.scripts
            for path in args_scripts:
                if path in model_scripts:
                    scripts.append(FalScript(model, path))
    else:
        real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
        for model in models:
            model_scripts: List[Path] = model.get_script_paths(
                args.keyword, real_project_dir, bool(args_dict.get("before"))
            )
            for path in model_scripts:
                scripts.append(FalScript(model, path))

    return scripts


def _run_global_scripts(project: FalProject, faldbt: FalDbt, global_key: str):
    global_scripts = list(
        map(
            lambda path: FalScript(None, Path(path)),
            faldbt._global_script_paths[global_key],
        )
    )

    run_global_scripts(global_scripts, project)
