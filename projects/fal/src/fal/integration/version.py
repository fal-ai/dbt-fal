import dbt.version
from dbt.semver import VersionSpecifier

def version_compare(version_string: str):
    return DBT_VCURRENT.compare(VersionSpecifier.from_version_string(version_string))

def is_version_plus(version_string: str):
    return version_compare(version_string) >= 0

DBT_VCURRENT = dbt.version.installed
