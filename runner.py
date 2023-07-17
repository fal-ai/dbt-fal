from dbt.cli.main import dbtRunner
from dbt.events.base_types import EventMsg

def print_callback(event: EventMsg):
    print("Got an event:", event.info.name)

dbt = dbtRunner()
dbt.callbacks.append(print_callback)

runresult = dbt.invoke(["parse"])
runresult.result
