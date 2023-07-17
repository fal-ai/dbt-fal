# dbt-fal: do more with dbt

dbt-fal is the easiest way to run Python with your [dbt](https://www.getdbt.com/) project.

<p align="center">
  <a href="https://fal.ai#signup">
    <img src="https://badgen.net/badge/icon/Sign%20up%20for%20fal%20Cloud/purple?icon=terminal&label" alt="fal Cloud" />
  </a>&nbsp;
  <a href="https://pepy.tech/project/fal">
    <img src="https://static.pepy.tech/personalized-badge/fal?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads" alt="Total downloads" />
  </a>&nbsp;
  <a href="https://pypi.org/project/fal/">
    <img src="https://badge.fury.io/py/fal.svg" alt="fal on PyPI" />
  </a>&nbsp;
  <a href="https://getdbt.slack.com/archives/C02V8QW3Q4Q">
    <img src="https://badgen.net/badge/icon/%23tools-fal%20on%20dbt%20Slack/orange?icon=slack&label" alt="Slack channel" />
  </a>&nbsp;
  <a href="https://discord.com/invite/Fyc9PwrccF">
    <img src="https://badgen.net/badge/icon/Join%20us%20on%20Discord/red?icon=discord&label" alt="Discord conversation" />
  </a>
</p>

# Introduction - 📖 [README](./projects/adapter)

The dbt-fal ecosystem has two main components: The command line and the adapter.

## CLI

With the CLI, you can:

- [Send Slack notifications](https://github.com/fal-ai/fal/tree/main/examples/slack-example) upon dbt model success or failure.
- [Load data from external data sources](https://blog.fal.ai/populate-dbt-models-with-csv-data/) before a model starts running.
- [Download dbt models](https://docs.fal.ai/fal/python-package) into a Python context with a familiar syntax: `ref('my_dbt_model')` using `FalDbt`
- [Programatically access rich metadata](https://docs.fal.ai/fal/reference/variables-and-functions) about your dbt project.

## Python Adapter

With the Python adapter, you can:

- Enable a developer-friendly Python environment for most databases, including ones without dbt Python support such as Redshift, Postgres.
- Use Python libraries such as [`sklearn`](https://scikit-learn.org/) or [`prophet`](https://facebook.github.io/prophet/) to build more complex `dbt` models including ML models.
- Easily manage your Python environments with [`isolate`](https://github.com/fal-ai/isolate).
- Iterate on your Python models locally and then [scale them out in the cloud](https://fal.ai#signup).

# Why are we building this?

We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of `dbt`. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates well with dbt.

# Have feedback or need help?

- Join us in [fal on Discord](https://discord.com/invite/Fyc9PwrccF)
- Join the [dbt Community](http://community.getdbt.com/) and go into our [#tools-fal channel](https://getdbt.slack.com/archives/C02V8QW3Q4Q)
