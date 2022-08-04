import importlib
from fal.typing import *
from _fal_testing import create_dynamic_artifact

create_dynamic_artifact(context)

try:
    importlib.import_module(context.arguments["import"])
except ImportError:
    import_success = False
else:
    import_success = True

assert import_success is context.arguments["expected_success"]
