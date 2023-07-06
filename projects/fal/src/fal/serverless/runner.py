from dataclasses import dataclass, field
import importlib
from pathlib import Path
from typing import List
from fal_serverless import isolated, sync_dir
import os


@isolated()
def serverless_runner(project_dir, profiles_dir, command, args: List[str] = []):
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()

    cli_args = [
        command,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
    ]
    cli_args.extend(args)

    runner.invoke(cli_args)


@dataclass
class FalServerlessRunner:
    project_dir: str
    profiles_dir: str
    dbt_version: str = field(init=False)
    data_project_dir: str = field(init=False)
    data_profiles_dir: str = field(init=False)

    def _sync_directories(self):
        sync_dir(
            self.project_dir,
            self.data_project_dir,
        )
        sync_dir(self.profiles_dir, self.data_profiles_dir)

    def __post_init__(self):
        from dbt.cli.main import dbtRunner

        cli_args = [
            "--project-dir",
            self.project_dir,
            "--profiles-dir",
            self.profiles_dir,
        ]
        runner = dbtRunner()
        metadata = runner.invoke(["parse"] + cli_args).result.metadata
        plugin_name = metadata.adapter_type

        try:
            mod = importlib.import_module(f"dbt.adapters.{plugin_name}.__version__")
        except ImportError:
            raise ValueError(
                f"Could not determine which adapter version is being used: {plugin_name}"
            )

        # easiest way to get the version, there might be better ways in the future
        dbt_version = f"dbt-{plugin_name}=={mod.version}"

        self.data_project_dir = str(Path("/data") / os.path.basename(self.project_dir))
        self.data_profiles_dir = str(
            Path("/data") / os.path.basename(self.profiles_dir)
        )
        self.dbt_version = dbt_version

    def seed(self, args: List[str] = [], sync: bool = True):
        if sync:
            self._sync_directories()

        serverless_runner.on(requirements=[self.dbt_version])(
            self.data_project_dir, self.data_profiles_dir, "seed", args
        )

    def run(self, args: List[str] = [], sync: bool = True):
        if sync:
            self._sync_directories()

        serverless_runner.on(requirements=[self.dbt_version])(
            self.data_project_dir, self.data_profiles_dir, "run", args
        )
