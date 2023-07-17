DBT_FAL_IMPORT_NOTICE = \
"""The dbt tool `fal` and `dbt-fal` adapter have been merged into a single tool.
Please import from the `fal.dbt` module instead.
Running `pip install dbt-fal` will install the new tool and the adapter alongside.
Then import from the `fal.dbt` module like

    from fal.dbt import {name}
"""

global DBT_FAL_IMPORT_NOTICE_SHOWN
DBT_FAL_IMPORT_NOTICE_SHOWN = {}

def _warn(name: str):
    # Print once per deprecated import
    global DBT_FAL_IMPORT_NOTICE_SHOWN
    if not DBT_FAL_IMPORT_NOTICE_SHOWN.get(name, False):
        print(DBT_FAL_IMPORT_NOTICE.format(name=name))
        DBT_FAL_IMPORT_NOTICE_SHOWN[name] = True

# Avoid printing on non-direct imports
def __getattr__(name: str):
    if name == "NodeStatus":
        _warn(name)
        from fal.dbt import NodeStatus
        return NodeStatus
    elif name == "FalDbt":
        _warn(name)
        from fal.dbt import FalDbt
        return FalDbt
    elif name == "DbtModel":
        _warn(name)
        from fal.dbt import DbtModel
        return DbtModel
    elif name == "DbtSource":
        _warn(name)
        from fal.dbt import DbtSource
        return DbtSource
    elif name == "DbtTest":
        _warn(name)
        from fal.dbt import DbtTest
        return DbtTest
    elif name == "DbtGenericTest":
        _warn(name)
        from fal.dbt import DbtGenericTest
        return DbtGenericTest
    elif name == "DbtSingularTest":
        _warn(name)
        from fal.dbt import DbtSingularTest
        return DbtSingularTest
    elif name == "Context":
        _warn(name)
        from fal.dbt import Context
        return Context
    elif name == "CurrentModel":
        _warn(name)
        from fal.dbt import CurrentModel
        return CurrentModel

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
