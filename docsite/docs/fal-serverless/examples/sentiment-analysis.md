---
sidebar_position: 1
---

# Sentiment Analysis with dbt

Sentiment analysis is the process of determining the sentiment or emotion behind a piece of text. It is widely used in social media monitoring, customer service, and marketing. By using sentiment analysis, you can quickly identify and respond to any complaints or other negative feedback.

This is a simple tutorial on how to perform sentiment analysis on a string using dbt fal-serverless.

### 1. Install fal-serverless and dbt-fal:

```python
pip install fal-serverless dbt-fal[snowflake]
```

### 2. Authenticate to fal-serverless:

```
fal-serverless auth login
```

### 3. Generate keys to access fal-serverless

```
fal-serverless key generate
```

### 4. Update your dbt profiles.yml

```
fal_profile:
  target: fal_serverless
  outputs:
    fal_serverless:
      type: fal
      db_profile: db
      host: <ask the fal team>
      key_secret: MY_KEY_SECRET_VALUE
      key_id: MY_KEY_ID_VALUE
    db:
      type: snowflake
      username: USERNAME
      password: PASSWORD
```

### 5. Create a sentiment-analysis fal environment

```
environments:
  - name: sentiment-analysis
    type: venv
    requirements:
      - transformers
      - torch
```

### 6. Define your dbt model:

```python
def model(dbt, fal):
    dbt.config(materialized="table")
    dbt.config(fal_environment="sentiment-analysis")
    dbt.config(fal_machine="GPU")
    from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
    import numpy as np
    import pandas as pd
    import torch

    # Check if a GPU is available and set the device index
    device_index = 0 if torch.cuda.is_available() else -1

    # Load the model and tokenizer
    model_name = "distilbert-base-uncased-finetuned-sst-2-english"
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    # Create the sentiment-analysis pipeline with the specified device
    classifier = pipeline("sentiment-analysis", model=model_name, tokenizer=tokenizer, device=device_index)

    ticket_data = dbt.ref("zendesk_ticket_data")
    ticket_descriptions = ticket_data["DESCRIPTION"].tolist()

    # Run the sentiment analysis on the ticket descriptions
    description_sentiment_analysis = classifier(ticket_descriptions)
    rows = []

    for id, sentiment in zip(ticket_data.ID, description_sentiment_analysis):
        rows.append((int(id), sentiment["label"], sentiment["score"]))

    records = np.array(rows, dtype=[("id", int), ("label", "U8"), ("score", float)])

    sentiment_df = pd.DataFrame.from_records(records)

    return sentiment_df
```

### 4. Run dbt:

```
dbt run
```

That's it. Doing a dbt run against this profile will execute your Python models in fal-serverless.

Of course, this is not the only way to run sentiment analysis on fal-serverless. There are many other libraries, APIs and techniques that can be run on fal-serverless.
