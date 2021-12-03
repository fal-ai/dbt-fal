# Example 4: Run sentiment analysis using HuggingFace and store results in your data warehouse

Sentiment Analysis is a powerful technique that takes advantage of NLP to understand opinions based on text. It is usually used to identify customer satisfaction. For simplicity we are taking advantage of a pretrained model and going to use [HuggingFace transformers for sentiment analysis](https://huggingface.co/transformers/quicktour.html).

We are going to use sample data from the [Fivetran dbt-zendesk](https://github.com/fivetran/dbt_zendesk/tree/main/integration_tests/data) repo to classify sentiment of fake customer support ticket reviews.

## Meta tag

In a `schema.yml` file, within a target model, a meta tag should be added in order to connect the model to fal:

```yaml
# models/shema.yml

models:
  - name: stg_zendesk_ticket_data
    description: zendesk ticket data
    config:
    materialized: table
    meta:
      fal:
        scripts:
          - "models/zendesk_sentiment_analysis.py"
```

## Seeding data to the warehouse

If you are just following this example for some practice with fal or dbt, one useful feature of dbt is to [seed csv data to your warehouse](https://docs.getdbt.com/docs/building-a-dbt-project/seeds). Instead of adding the dummy data to your warehouse you can put [Fivetran dbt-zendesk](https://github.com/fivetran/dbt_zendesk/tree/main/integration_tests/data) ticket data in the data folder of your project and run `dbt seed`. Just a friendly warning that seeding should only be used with small amount of data.

Alternatively you can load this data to your warehouse with in any way as you like.

## Using ref() and transformer

Let's first install the transformer library from hugging face. Head to your terminal and the python environment that you have installed `fal` and run:

```
pip install transformers pandas numpy
```

(We are working on better dependency managment, head over to this [github issue](https://github.com/fal-ai/fal/issues/10) if you run into any problems with this step)

Once the transformer dependency is installed create a python file in the location that is specified above in your `schema.yml` file, `models/zendesk_sentiment_analysis.py`.

```py
# models/zendesk_sentiment_analysis.py

from transformers import pipeline

ticket_data = ref("stg_zendesk_ticket_data")
ticket_descriptions = list(ticket_data.description)
classifier = pipeline("sentiment-analysis")
description_sentimet_analysis = classifier(ticket_descriptions)
```

## Writing the analysis results back to your warehouse

To upload data back to the warehouse, we define a source where we will be uploading it to.
We upload to a source because it may need more dbt transformations afterwards, and a source is the perfect place for that.

```yaml
# models/shema.yml [continuation]

sources:
  - name: results
    tables:
      - name: ticket_data_sentiment_analysis
```

Then let's organize the resulting data frame before uploading it

```py
# models/zendesk_sentiment_analysis.py [continuation]

rows = []
for id, sentiment in zip(ticket_data.id, description_sentimet_analysis):
    rows.append((int(id), sentiment["label"], sentiment["score"]))

records = np.array(rows, dtype=[("id", int), ("label", "U8"), ("score", float)])

sentiment_df = pd.DataFrame.from_records(records)
```

And finally, upload it with the handy `write_to_source` function

```py
# models/zendesk_sentiment_analysis.py [continuation]

print("Uploading\n", sentiment_df)
write_to_source(sentiment_df, "results", "ticket_data_sentiment_analysis")
```

The table `ticket_data_sentiment_analysis` will be created if it's not already present, and in case
it existed and there was data already, data will be appended to it.

It can be used from dbt as a regular source with the usual `{{ source('results', 'ticket_data_sentiment_analysis') }}` syntax.
