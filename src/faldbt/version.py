import dbt.version
from dbt.semver import VersionSpecifier

def version_compare(version_string: str):
    return DBT_VCURRENT.compare(VersionSpecifier.from_version_string(version_string))

DBT_VCURRENT = dbt.version.installed
IS_DBT_V1PLUS = version_compare("1.0.0") >= 0
IS_DBT_WITH_PYTHON_MODELS = version_compare("1.3.0-a1") >= 0
