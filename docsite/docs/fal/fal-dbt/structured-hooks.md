# Passing parametrized data around hooks

Aside from the context around the model that it is bound to, each Fal
hook (hooks defined under `pre-hook` and `post-hook`) can have a set
of parameters that would allow it to be parametrized in a per-usage
basis (rather than having multiple duplicate scripts with statically
embedded configuration values). This customization can be applied by
using the `with:` section, for example like the `send_alert.py` below:

```yml
models:
  - name: normalize_balances
    meta:
      fal:
        post-hook:
          - my_regular_hook.py
          - path: send_alert.py
            with:
              channel: "#finance-alerts"
              severity: 1

   - name: revoke_account_permissions
     meta:
       fal:
         post-hook:
           - path: send_alert.py
             with:
               channel: "#hr-alerts"
               severity: 0
```

A hook is a dictionary with a `path` and `with` properties. If a string is set
instead of the dictionary format, it is assumed as the `path` property.

In our example, we keep the same script for sending all alerts (`send_alert.py`) but we customize
which channel we want to send an alert on depending on the model that we are processing by simply
passing that extra piece of information under the `with:` section. We can also pass different types
like how the `severity` option is an integer, and they will be exposed to our hooks with the original
types (`channel` is going to be a `str`, and `severity` is going to be an `int`).

For leveraging this set of parameters in our hooks, we can use the existing `context` object but this time
accessing the `context.arguments` property:

```py
from fal.typing import *
from my_alerts import send_slack_msg, send_sentry_alert

channel = context.arguments["channel"]
# We can use `get` to set a default value for an argument
severity = context.arguments.get("severity", 1)

if context.current_model.status == "fail":
    message = "{context.current_model.name} has failed"
    send_slack_msg(to=channel, message=message)
    if severity >= 1:
        send_sentry_alert(message)
```
