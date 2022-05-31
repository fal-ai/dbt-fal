"""Example workflow with fal."""
from fal_dagster import fal_resource, flow_run

do_fal_flow_run_job = flow_run.to_job(resource_defs={"fal": fal_resource})
