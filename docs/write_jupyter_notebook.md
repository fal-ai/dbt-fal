# Example 9: Use dbt from a Jupyter Notebook

We also offer fal as an importable package to load in a Python environment to reference and use your dbt tables in a dynamic way.

We start by importing fal into your project

```py
from fal import FalDbt
```

Then instantiate a new FalDbt project with the dbt project information:

```py
faldbt = FalDbt(profiles_dir="~/.dbt", project_dir="../my_project")
print("Sources", faldbt.list_sources())
# [['results', 'ticket_data_sentiment_analysis']]
print("Models", faldbt.list_models())
# {
#   'zendesk_ticket_metrics': <RunStatus.Success: 'success'>, 
#   'stg_o3values': <RunStatus.Success: 'success'>, 
#   'stg_zendesk_ticket_data': <RunStatus.Success: 'success'>, 
#   'stg_counties': <RunStatus.Success: 'success'>
# }
```

And finally, just reference these objects as you would in a regular fal script, from the `faldbt` object:

```py
sentiments = faldbt.source('results', 'ticket_data_sentiment_analysis')
# pandas.DataFrame
tickets = faldbt.ref('stg_zendesk_ticket_data')
# pandas.DataFrame
```

You can use any other function available in the fal script runtime through the `faldbt` object.
