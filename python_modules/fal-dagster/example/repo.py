"""Example workflow with fal."""
from fal_de import fal_resource, flow_run
from dagster_docker import docker_executor

do_fal_flow_run_job = flow_run.to_job(
    resource_defs={"fal": fal_resource}, executor_def=docker_executor
)
