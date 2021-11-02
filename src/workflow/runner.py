import yaml
from pydantic import ValidationError

import subprocess
import shlex

from ..workflow import StepContext
from ..workflow import PythonClassStep, Workflow, BashStep

from Jinja2 import Template

import os


class Runner:
    def __init__(self):
        self.global_context = {"steps": {}}
        return

    def _substitute_variables(self, cmd: str):
        tm = Template(cmd)
        template_context = {}

        for step_name, step_context in self.global_context["steps"].items():
            template_context[step_name] = step_context.get_dict()

        msg = tm.render(steps=template_context)
        return msg

    def _set_env(self, env) -> None:
        for k, v in env.items():
            os.environ[k] = v
        return

    def run_simple_step(self, step) -> None:
        step_context = StepContext()

        cmd = self._substitute_variables(step.run)
        print("    #### CMD:", cmd)
        process = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        step_context.set_output("stdout", stdout.decode("utf-8").strip())
        step_context.set_output("stderr", stderr.decode("utf-8").strip())

        self.global_context["steps"][step.name] = step_context
        return

    def run_python_class_step(self, step) -> None:
        # Get with variables
        inputs = {
            key: self._substitute_variables(value) for key, value in step.with_.items()
        }

        step_context = StepContext({"inputs": inputs})

        # Dynamically load action class
        # format: actions.slack.slack_action.SlackAction
        python_class = step.python_class.split(".")
        cls = python_class[-1]
        mdl = ".".join(python_class[:-1])

        module = __import__(mdl, fromlist=[""])
        # from actions.slack.slack_action import SlackAction
        action_cls = getattr(module, cls)
        action_cls().run(step_context)

        # Add context to global context
        self.global_context["steps"][step.name] = step_context
        return

    def run(self, workflow_file):
        # read workflow file
        with open(workflow_file, "r") as file:
            workflow_data = yaml.safe_load(file)

        try:
            workflow = Workflow(**workflow_data)
        except ValidationError as e:
            print(e.json())

        for job_id in workflow.jobs:
            print(f"# Running job: {job_id}")
            job = workflow.jobs[job_id]
            self._set_env(job.env)

            for step in job.steps:
                print(f"  * Running step: {step.name}")
                self._set_env(step.env)

                if isinstance(step, BashStep):
                    self.run_simple_step(step)
                elif isinstance(step, PythonClassStep):
                    self.run_python_class_step(step)
                else:
                    raise Exception

                # Debug: To see context
                # pprint.pprint(self.global_context)
        return workflow
