from __future__ import annotations

import glob
import importlib
import importlib.util
import os
from pathlib import Path
from typing import Iterator, Optional, Tuple


# NOTE: This is from dbt https://github.com/dbt-labs/dbt-core/blob/7bd861a3514f70b64d5a6c642b4204b50d0d3f7e/core/dbt/version.py#L209-L236
def _get_dbt_plugins_info() -> Iterator[Tuple[str, str]]:
    for plugin_name in _get_adapter_plugin_names():
        if plugin_name == "core":
            continue
        try:
            mod = importlib.import_module(f"dbt.adapters.{plugin_name}.__version__")
        except ImportError:
            # not an adapter
            continue
        yield plugin_name, mod.version  # type: ignore


def _get_adapter_plugin_names() -> Iterator[str]:
    spec = importlib.util.find_spec("dbt.adapters")
    # If None, then nothing provides an importable 'dbt.adapters', so we will
    # not be reporting plugin versions today
    if spec is None or spec.submodule_search_locations is None:
        return

    for adapters_path in spec.submodule_search_locations:
        version_glob = os.path.join(adapters_path, "*", "__version__.py")
        for version_path in glob.glob(version_glob):
            # the path is like .../dbt/adapters/{plugin_name}/__version__.py
            # except it could be \\ on windows!
            plugin_root, _ = os.path.split(version_path)
            _, plugin_name = os.path.split(plugin_root)
            yield plugin_name


def get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
    import dbt.version
    import pkg_resources
    from dbt.semver import VersionSpecifier

    raw_fal_version = pkg_resources.get_distribution("fal").version
    fal_version = VersionSpecifier.from_version_string(raw_fal_version)
    if fal_version.prerelease:
        import fal

        # If this is a development version, we'll install
        # the current fal itself.
        base_dir = Path(fal.__file__).parent.parent.parent
        assert (base_dir / ".git").exists()
        yield str(base_dir), None
    else:
        yield "fal", raw_fal_version

    yield "dbt-core", dbt.version.__version__

    for adapter_name, adapter_version in _get_dbt_plugins_info():
        yield f"dbt-{adapter_name}", adapter_version
