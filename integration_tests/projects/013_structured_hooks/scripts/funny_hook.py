from fal.typing import *
from _fal_testing import create_dynamic_artifact

import pyjokes

print("While we are preparing your artifacts, here is a joke for you: ", pyjokes.get_joke())

create_dynamic_artifact(context, f"PyJokes version: {pyjokes.__version__}")

