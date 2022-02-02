import asyncio
import logging
from datetime import timedelta

from temporal.activity_method import activity_method
from temporal.workerfactory import WorkerFactory
from temporal.workflow import workflow_method, Workflow, WorkflowClient

logging.basicConfig(level=logging.INFO)

TASK_QUEUE = "DBT-FLOW-PYTHON"
NAMESPACE = "***"

# Activities Interface
class FalScriptActivities:
    @activity_method(task_queue=TASK_QUEUE, schedule_to_close_timeout=timedelta(seconds=1000))
    async def compose_greeting(self, greeting: str) -> str:
        raise NotImplementedError


# Activities Implementation
class FalScriptActivitiesImpl:
    async def run_script(self, greeting: str):
        return "FROM PYTHON: " + greeting + "!"

async def client_main():
    client = WorkflowClient.new_client(host="***", port=80, namespace=NAMESPACE)

    factory = WorkerFactory(client, NAMESPACE)
    worker = factory.new_worker(TASK_QUEUE)
    worker.register_activities_implementation(FalScriptActivitiesImpl(), "FalScriptActivities")
    factory.start()
    print("starting workers.....")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(client_main())
    loop.run_forever()
