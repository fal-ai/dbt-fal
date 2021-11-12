# Example 1: Send a Slack message about model status
You can use fal to send Slack messages.

## Setting up a Slack App:

### 1. Create a Slack App

Follow instructions on this page in order to create an organization-specific Slack app:

https://slack.com/help/articles/115005265703-Create-a-bot-for-your-workspace

Add following OAuth Scopes:

- `channels:join`
- `chat:write`
- `files:write`
- `app_mentions:read`
- `groups:read`

### 2. Install the app and get the bot token

On the same "OAuth & Permissions" page, click on "Install to Workspace" button, proceed with installation and take note of the provided Bot User OAuth Token.

### 3. Get channel ID

In Slack, right click on the channel that you want fal to publish to, click on "Open channel details" and copy the Channel ID on the bottom of the modal.

### 4. Add your bot to your channel

In your Slack channel, type following message:

`/add @your_bot_name`

### 5. Set environment variables

In terminal set following two environment variable: `SLACK_BOT_TOKEN` and `SLACK_BOT_CHANNEL`. This can be done with export command:

```bash
export SLACK_BOT_TOKEN=your-bot-token
export SLACK_TARGET_CHANNEL=your-target-channel
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

This example requires [`slack_sdk`](https://github.com/slackapi/python-slack-sdk) to be installed. We will be using the `WebClient` class for sending messages to our Slack app:

```python
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

CHANNEL_ID = os.getenv("SLACK_BOT_CHANNEL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")

client = WebClient(token=SLACK_TOKEN)
```

Fal provides a magic variable `context` that gives you access to dbt model information, such as model name and status. We can create a message using this variable:
```python
message_text = f"Model: {context.current_model.name}. Status: {context.current_model.status}."
```

And finally we post this message to our target Slack channel:
```python
try:
    response = client.chat_postMessage(
        channel=CHANNEL_ID,
        text=message_text
    )
except SlackApiError as e:
    assert e.response["error"]
```

You can find the full code example [here](scripts/slack_example.py).
