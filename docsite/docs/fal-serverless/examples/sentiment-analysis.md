---
sidebar_position: 1
---

# Sentiment Analysis

Sentiment analysis is the process of determining the sentiment or emotion behind a piece of text. It is widely used in social media monitoring, customer service, and marketing. By using sentiment analysis, you can quickly identify and respond to any complaints or other negative feedback.

This is a simple tutorial on how to perform sentiment analysis on a string using fal-serverless.

### 1. Import isolated decorator:

```python
from fal_serverless import isolated
```

### 2. Define requirements list:

```python
requirements = ["transformers==4.26.0", "torch==1.13.1"]
```

### 3. Define an isolated function:

```python
# Set machine_type="M" for more RAM
@isolated(requirements=requirements, machine_type="M")
def do_sentiment_analysis(input: str) -> list[dict]:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

    # Download a sentiment analysis model
    model = AutoModelForSequenceClassification.from_pretrained(
        "pysentimiento/robertuito-sentiment-analysis", cache_dir="/data/huggingface")

    # Download a tokenizer
    tokenizer = AutoTokenizer.from_pretrained(
        "pysentimiento/robertuito-sentiment-analysis", cache_dir="/data/huggingface")

    # Initialize pipeline
    pipe = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

    # Run analysis and immediately return
    return pipe(input)
```

Inside the `do_sentiment_analysis` function definition we are downloading a model from Hugging Face in this line:

```python
    model = AutoModelForSequenceClassification.from_pretrained(
        "pysentimiento/robertuito-sentiment-analysis", cache_dir="/data/huggingface")
```

By specifying the `cache_dir`, we are making sure that we don't have to download the model repeatedly. It will be stored inside our `/data` directory that works as a user-specific and persistent cache.

### 4. Call the isolated function with an input string

```python
result = do_sentiment_analysis("This is a totally awesome sentence, I couldn't be happier about it!")
```

The first time `do_sentiment_analysis` is called, it will likely take a bit of time, since it will need to install depedencies and then download the model data. Subsequent runs should be much faster, as fal-serverless smartly caches both the target environment and the user's working directory.

Of course, this is not the only way to run sentiment analysis on fal-serverless. There are many other libraries, APIs and techniques that can be run on fal-serverless.
