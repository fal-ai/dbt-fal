import pprint
from typing import Any

def _merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z

class StepContext:
    def __init__(self, additional_context = {}):
        default_context = {
            "inputs"  : {},
            "outputs" : {}
        }
        self.context = _merge_two_dicts(
            default_context, 
            additional_context
        )
        return

    def get_dict(self):
        return self.context
    
    def __repr__(self):
        return pprint.pformat(self.context, indent=4)

    def get_input(self, input_key) -> str:
        return self.context["inputs"][input_key]

    def set_output(self, output_key, output_value) -> None:
        self.context["outputs"][output_key] = output_value
        return
