# Example 8: Send Datadog event

## Setting up Datadog WebAPI

Get [API and Application keys](https://docs.datadoghq.com/account_management/api-app-keys/) for your Datadog account. Set them up as environment variables:

```bash
export DD_API_KEY=your-api-key
export DD_APP_KEY=your-app-key
```

Install the Datadog client, if you haven't done so already:

```bash
pip install datadog_api_client
```

## Meta tag

In a `schema.yml` file, within a target model, a meta tag should be added in order to connect the model to fal:

```yaml
meta:
  fal:
    scripts:
      - path_to_fal_script.py
```

## Fal script

Import necessary libraries and setup the Datadog application configurations:

```python
from datadog_api_client.v1 import ApiClient, ApiException, Configuration
from datadog_api_client.v1.api import events_api
from datadog_api_client.v1.models import EventCreateRequest
import os
import time
import io

current_time = time.time()

configuration = Configuration()
configuration.api_key['apiKeyAuth'] = os.getenv("DD_API_KEY")
configuration.api_key['appKeyAuth'] = os.getenv("DD_APP_KEY")
```

Get you model as a pandas DataFrame by using the `ref` function and the `context` variable:

```python
df = ref(context.current_model.name)
```

Prepare event message:

```python
buf = io.StringIO()
df.info(buf=buf)

text = buf.getvalue()
tags = ["fal"]

event_body = EventCreateRequest(
    tags=tags,
    aggregation_key="fal",
    title="fal - event",
    text=text,
    date_happened=int(current_time)
)
```

Send event to Datadog:

```python
with ApiClient(configuration) as api_client:
    # Create an instance of the API class
    events_api_instance = events_api.EventsApi(api_client)
    try:
        events_api_instance.create_event(event_body)
    except ApiException as e:
        assert e.response["error"]
```

## Full example

You can find the full code example [here](https://github.com/fal-ai/fal_dbt_examples/blob/main/fal_scripts/send_datadog_event.py).
