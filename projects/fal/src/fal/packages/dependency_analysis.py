from __future__ import annotations

from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import importlib_metadata

from fal.utils import cache_static

import importlib_metadata


def _is_fal_pre_release() -> bool:
    from dbt.semver import VersionSpecifier

    raw_fal_version = importlib_metadata.version("fal")
    fal_version = VersionSpecifier.from_version_string(raw_fal_version)
    return fal_version.prerelease


def _get_fal_root_path() -> Path:
    import fal

    # If this is a development version, we'll install
    # the current fal itself.
    base_dir = Path(fal.__file__).parent.parent.parent
    assert (base_dir / ".git").exists()
    return base_dir


def _get_dbt_packages() -> Iterator[Tuple[str, Optional[str]]]:
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
        distribution = importlib_metadata.distribution(dbt_plugin_name)

        # Handle dbt-fal separately (since it needs to be installed
        # with its extras).
        if dbt_plugin_name == "dbt-fal":
            continue

        yield dbt_plugin_name, distribution.version

    try:
        dbt_fal_version = importlib_metadata.version("dbt-fal")
    except importlib_metadata.PackageNotFoundError:
        # It might not be installed.
        return None

    if _is_fal_pre_release():
        dbt_fal_dep = str(_get_fal_root_path() / "adapter")
        # We are going to install it from the local path.
        dbt_fal_version = None
    else:
        dbt_fal_dep = "dbt-fal"

    dbt_fal_extras = _find_fal_extras("dbt-fal")
    if dbt_fal_extras:
        dbt_fal_dep += f"[{' ,'.join(dbt_fal_extras)}]"
    yield dbt_fal_dep, dbt_fal_version


def _find_fal_extras(package: str) -> Iterator[str]:
    # Return a possible set of extras that might be required when installing
    # fal in the new environment. The original form which the user has installed
    # is not present to us (it is not saved anywhere during the package installation
    # process, so there is no way for us to know how a user initially installed fal).
    # We'll therefore be as generous as possible and install all the extras for all
    # the dbt.adapters that the user currently has (so this will still be a subset
    # of dependencies, e.g. if there is no dbt.adapter.duckdb then we won't include
    # duckdb as an extra).

    import pkgutil

    import dbt.adapters

    fal_dist = importlib_metadata.distribution(package)
    all_extras = fal_dist.metadata.get_all("Provides-Extra", [])

    # This list is different from the one we obtain in _get_dbt_packages
    # since the names here are the actual import names, not the PyPI names
    # (e.g. this one will say athena, and the other one will say dbt-athena-adapter).
    available_dbt_adapters = {
        module_info.name
        for module_info in pkgutil.iter_modules(dbt.adapters.__path__)
        if module_info.ispkg
    }

    # There will be adapters which we won't have an extra for (e.g. oraceledb)
    # and there will be extras which the user did not install the adapter for
    # (e.g. dbt-redshift). We want to take the intersection of all the adapters
    # that the user has installed and all the extras that fal provides and find
    # the smallest possible subset of extras that we can install.
    return available_dbt_adapters.intersection(all_extras)


def _get_fal_package_name() -> Tuple[str, Optional[str]]:
    if _is_fal_pre_release():
        fal_dep, fal_version = str(_get_fal_root_path()), None
    else:
        fal_dep, fal_version = "fal", importlib_metadata.version("fal")

    fal_package_name = fal_dep
    fal_extras = _find_fal_extras("fal")
    if fal_extras:
        fal_package_name += f"[{' ,'.join(fal_extras)}]"
    return fal_package_name, fal_version


def get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
    yield _get_fal_package_name()
    yield from _get_dbt_packages()


@cache_static
def get_default_pip_dependencies() -> List[str]:
    return [
        f"{package}=={version}" if version else package
        for package, version in get_default_requirements()
    ]
