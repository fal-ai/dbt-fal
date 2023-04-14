---
sidebar_position: 6
---

# Scheduling runs

Functions can also be scheduled to run at specific times and/or frequency using cron expressions.

The command to schedule function expects a cron expression, a path to a python file containing an `@isolated` function, and the function name.

```
fal-serverless function schedule <CRON> <PATH_TO_FILE> <FUNCTION_NAME>
```

## Example

Let's write a fun example. How about a Slack bot that sends the joke of the day to a Slack channel.

1. First write your `@isolated` function:

```python
@isolated(requirements=["pyjokes", "httpx"])
def joke_of_the_day():
    import httpx
    import pyjokes

    joke = pyjokes.get_joke()
    message = f"Hello! I'm the funny bot, here's the joke of the day:\n> {joke}"
    
    slack_token = "<YOUR_SLACK_TOKEN>"
    slack_webhook_url = f"https://hooks.slack.com/services/{slack_token}"
    httpx.post(slack_webhook_url, json={"text": message})
```

2. Schedule it so it's executed once a day at 7am UTC:

```
fal-serverless function schedule "0 7 * * *" jokes.py joke_of_the_day
```

*Voilà!* That's all that it takes to have your `@isolated` function scheduled.

**Tip:** if you're not familiar with cron expressions, this [cron editor](https://crontab.guru/) can help you get started.


## Managing scheduled functions

Once scheduled, functions can be managed with a few commands. Let's check them out.

### List all scheduled functions

Get a list of all currently scheduled functions with:

```
fal-serverless crons list
```

### Cancel scheduling

In order to cancel a scheduled function, run:

```
fal-serverless crons cancel <CRON_ID>
```

### List activations

You can get the history of a particular function activation (i.e. execution) with:

```
fal-serverless crons activations <CRON_ID>
```

You can also specify `--limit=n` to list the `n` most recent activations. It defaults to 15.

## Logs

Since scheduled functions run asynchronously, logs are stored so they can be retrieved later. This is particularly useful for debugging executions. Get the logs for a particular activation with:

```
fal-serverless crons logs <CRON_ID> <ACTIVATION_ID>
```
