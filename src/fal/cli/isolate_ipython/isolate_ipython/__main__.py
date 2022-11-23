import argparse
import copy
import tempfile
import textwrap
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional

import isolate
from isolate.backends.local import LocalPythonEnvironment
from isolate.backends.virtualenv import PythonIPC

DEFINITIONS = {
    "ml": {
        "kind": "virtualenv",
        "requirements": [
            "pyjokes==0.5.0",
        ],
    }
}


def get_ipython_config(default_env: Dict[str, Any]) -> Path:
    config_dir = Path(tempfile.mkdtemp())
    default_profile_dir = config_dir / "profile_default"
    default_profile_dir.mkdir(parents=True, exist_ok=True)
    default_profile_dir = default_profile_dir / "ipython_config.py"
    default_profile_dir.write_text(
        textwrap.dedent(
            f"""\
                c.InteractiveShellApp.exec_lines = [
                    "_global_isolate_env={repr(default_env)}",
                    "from faldbt.magics import init_fal",
                    "get_ipython().magic('%init_fal project_dir=/Users/burkaygur/src/jaffle_shop_with_fal/ profiles_dir=~/.dbt')"
                ]
            """
        )
    )
    return config_dir


# def get_ipython_config(default_env: Dict[str, Any]) -> Path:
#     config_dir = Path(tempfile.mkdtemp())
#     default_profile_dir = config_dir / "profile_default"
#     default_profile_dir.mkdir(parents=True, exist_ok=True)

#     default_profile_dir = default_profile_dir / "ipython_config.py"
#     default_profile_dir.write_text(
#         textwrap.dedent(
#             f"""\
#                 #c.InteractiveShellApp.extensions.append('isolate_ipython')
#                 c.InteractiveShellApp.exec_lines = [
#                     "_global_isolate_env={repr(default_env)}"
#                 ]
#     """
#         )
#     )
#     return config_dir


def create_notebook(name: str) -> None:
    definition = copy.deepcopy(DEFINITIONS[name])
    ipython_config = get_ipython_config(definition)

    if definition["kind"] == "virtualenv":
        definition["requirements"].extend(["jupyterlab", "fal"])  # type: ignore
    else:
        raise NotImplementedError(definition["kind"])

    environment = isolate.prepare_environment(**definition)
    # with environment.connect() as connection:
    #     connection.run(
    #         partial(
    #             exec,
    #             "import os;"
    #             "import sys;"
    #             "from jupyterlab.labapp import main;"
    #             "sys.argv = ['jupyter'];"
    #             f"os.environ['IPYTHONDIR'] = '{ipython_config}';"
    #             "main()",
    #         )
    #     )

    with PythonIPC(
        environment,
        environment.create(),
        extra_inheritance_paths=[LocalPythonEnvironment().create()],
    ) as connection:
        result = connection.run(
            partial(
                exec,
                "import os;"
                "import sys;"
                "from jupyterlab.labapp import main;"
                "sys.argv = ['jupyter'];"
                f"os.environ['IPYTHONDIR'] = '{ipython_config}';"
                "main()",
            )
        )
        return result


def notebook_run(parsed: argparse.Namespace) -> int:
    create_notebook("ml")
    return 0
