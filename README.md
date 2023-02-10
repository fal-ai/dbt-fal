# fal: do more with dbt

fal is the easiest way to run Python with your [dbt](https://www.getdbt.com/) project.

**NEW:** Sign up for a private beta of [fal Cloud](https://fal.ai#signup)

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

Let's discover `fal` in less than 5 minutes:

<p align="center">
  <a href="https://www.loom.com/share/bb49fffaa6f74e90b91d26c77f35ecdc">
    <img align="center" src="https://cdn.loom.com/sessions/thumbnails/bb49fffaa6f74e90b91d26c77f35ecdc-1637262660876-with-play.jpg" alt="Intro video" />
  </a>
</p>

# Introduction

The fal ecosystem has two main components: The `fal` CLI and the `dbt-fal` adapter.

## `fal` – 📖 [README](./projects/fal)

With the `fal` CLI, you can:

- [Send Slack notifications](https://github.com/fal-ai/fal/tree/main/examples/slack-example) upon dbt model success or failure.
- [Load data from external data sources](https://blog.fal.ai/populate-dbt-models-with-csv-data/) before a model starts running.
- [Download dbt models](https://docs.fal.ai/fal/python-package) into a Python context with a familiar syntax: `ref('my_dbt_model')` using `FalDbt`
- [Programatically access rich metadata](https://docs.fal.ai/fal/reference/variables-and-functions) about your dbt project.

Check out the [`fal` README](./projects/fal) for more information.

For more details on `fal`, [go to the documentation](https://docs.fal.ai/)!

## `dbt-fal` – 📖 [README](./projects/adapter)

With the `dbt-fal` Python adapter, you can:

- Enable a developer-friendly Python environment for most databases, including ones without dbt Python support such as Redshift, Postgres.
- Use Python libraries such as [`sklearn`](https://scikit-learn.org/) or [`prophet`](https://facebook.github.io/prophet/) to build more complex `dbt` models including ML models.
- Easily manage your Python environments with [`isolate`](https://github.com/fal-ai/isolate).
- Iterate on your Python models locally and then [scale them out in the cloud](https://fal.ai#signup).

Check out the [`dbt-fal` README](./projects/adapter) for more information.

For more details on `dbt-fal`, [go to the documentation](https://docs.fal.ai/dbt-fal/quickstart)!

# Why are we building this?

We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with.

`dbt`'s SQL only design is powerful, but if you ever want to get out of SQL-land and connect to external services or get into Python-land for any reason, you will have a hard time. We built `fal` to enable Python workloads (sending alerts to Slack, building predictive models, pushing data to non-data-warehouse destinations and more) **right within `dbt`**.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of `dbt`. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates well with dbt.

# Have feedback or need help?

- Join us in [fal on Discord](https://discord.com/invite/Fyc9PwrccF)
- Join the [dbt Community](http://community.getdbt.com/) and go into our [#tools-fal channel](https://getdbt.slack.com/archives/C02V8QW3Q4Q)
