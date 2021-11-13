# Example 4: Run sentiment analysis using HuggingFace and store results in your data warehouse

Sentiment Analysis is a powerful tecnique that takes advantage of NLP to identify affective states. It is usually used to identify custumer satisfaction. For simplicity we are taking advantage of a pretrained model and going to use [HuggingFace transformers for sentiment analysis](https://huggingface.co/transformers/quicktour.html).

We are going to use sample data from the [Fivetran dbt-zendesk](https://github.com/fivetran/dbt_zendesk/tree/main/integration_tests/data) repo to classify sentiment of fake customer support ticket reviews.

## Meta tag

In a `schema.yml` file, within a target model, a meta tag should be added in order to connect the model to fal:

```yaml
    models:
    - name: stg_zendesk_ticket_data
        description: zendesk ticket data
        config:
        materialized: table
        meta:
        fal:
            scripts: ["models/zendesk_sentiment_analysis.py"]
```

## Seeding data to the warehouse

If you are just following this example for some practice with fal or dbt, one useful feature of dbt is to [seed csv data to your warehouse](https://docs.getdbt.com/docs/building-a-dbt-project/seeds). Instead of adding the dummy data to your warehouse you can put [Fivetran dbt-zendesk](https://github.com/fivetran/dbt_zendesk/tree/main/integration_tests/data) ticket data in the data folder of your project and run `dbt seed`. Just a friendly warning that seeding should only be used with small amount of data.

Alternatively you can load this data to your warehouse with in any as you like.

## Using ref() and transformer

Let's first install the transformer library from hugging face. Head to your terminal and the python environment that you have install `fal` and run:

```
pip install transformers
```

(We are working on better dependency managment, head over to this [github issue](https://github.com/fal-ai/fal/issues/10) if you run into any problems with this step)

Once the transformer dependency is installed create a python file in the location that is specified above in your `schema.yml` file, `models/zendesk_sentiment_analysis.py`.

```
from transformers import pipeline

ticket_descriptions = list(ref("stg_zendesk_ticket_data").description)
classifier = pipeline("sentiment-analysis")

results = classifier(ticket_descriptions), "sentiment_analysis")
```

## Writing the analysis results back to your warehouse

```
TODO
```
