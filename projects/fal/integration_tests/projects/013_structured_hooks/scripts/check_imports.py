import importlib
from fal.typing import *
from _fal_testing import create_dynamic_artifact

create_dynamic_artifact(context)

module_name = context.arguments["import"]
expectation = "succeed" if context.arguments["expected_success"] else "fail"

try:
    module = importlib.import_module(module_name)
except ImportError:
    result = "fail"
else:
    result = "succeed"

assert (
    expectation == result
), f"Expected import {module_name} to {expectation}, but it {result}."

if "version" in context.arguments:
    expected_version = context.arguments["version"]
    actual_version = module.__version__
    assert expected_version == actual_version, f"Expected version {expected_version} of {module_name}, but got {actual_version}."
