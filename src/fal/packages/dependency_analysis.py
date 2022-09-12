from __future__ import annotations

import importlib_metadata
from pathlib import Path
from typing import Iterator, Optional, Tuple


def _get_dbt_packages() -> Iterator[Tuple[str, str]]:
    # package_distributions will return a mapping of top-level package names to a list of distribution names (
    # the PyPI names instead of the import names). An example distirbution info is the following, which
    # contains both the main exporter of the top-level name (dbt-core) as well as all the packages that
    # export anything to that namespace:
    #   {"dbt": ["dbt-core", "dbt-postgres", "dbt-athena-adapter"]}
    #
    # This won't only include dbt.adapters.xxx, but anything that might export anything to the dbt namespace
    # (e.g. a hypothetical plugin that only exports stuff to dbt.includes.xxx) which in theory would allow us
    # to replicate the exact environment.
    package_distributions = importlib_metadata.packages_distributions()
    for dbt_plugin_name in package_distributions.get("dbt", []):
        yield dbt_plugin_name, importlib_metadata.version(dbt_plugin_name)


def get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
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

    yield from _get_dbt_packages()
