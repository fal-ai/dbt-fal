from typing import Optional, List, cast
from dbt.cli.main import dbtRunner, dbtRunnerResult

class falProject:
    """
    Represents a dbt project and access to its resources and utility functions.
    """

    def _dbt_invoke(
        self, cmd: str, args: Optional[List[str]] = None
    ) -> dbtRunnerResult:
        if args is None:
            args = []

        project_args = [
            "--project-dir",
            self.project_dir,
            "--profiles-dir",
            self.profiles_dir,
        ]
        if self.target_name:
            project_args.extend(["--target", self.target_name])

        # TODO: Intervene the dbt logs and capture them to avoid printing them to the console
        return self._runner.invoke([cmd] + project_args + args)

    def __init__(
        self, project_dir: str, profiles_dir: str, target_name: Optional[str] = None
    ):
        # TODO: Make project_dir and profiles_dir optional and use the current working directory and default profiles dir?
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target_name = target_name

        # Load the manifest information
        self._runner = dbtRunner()
        parse_result = self._dbt_invoke("parse")
        native_manifest = cast(Manifest, parse_result.result)  # type: ignore
        self._manifest = native_manifest
        # self._manifest = DbtManifest(native_manifest)

        # TODO: Do we need the manifest in there?
        self._runner = dbtRunner(manifest=native_manifest)
