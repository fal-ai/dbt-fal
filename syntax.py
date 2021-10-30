from pydantic import BaseModel
from typing import List, Dict, Union
from enum import Enum

class On(str, Enum):
    model_complete = 'model.success'
    model_fail = 'model.fail'

class BashStep(BaseModel):
    name: str
    run: str

class PythonClassStep(BaseModel):
    name: str
    python_class: str
    with_: Dict[str, str]

    class Config:
        fields = {
            'with_': 'with'
        }

class Job(BaseModel):
    steps: List[Union[BashStep, PythonClassStep]]

class Workflow(BaseModel):
    name: str
    on_event: On
    jobs: Dict[str, Job]

