# dbt-fal: do more with dbt

dbt-fal is the easiest way to run Python with your [dbt](https://www.getdbt.com/) project.

<p align="center">
  <a href="https://fal.ai">
    <img src="https://badgen.net/badge/icon/Sign%20up%20for%20fal%20Serverless/purple?icon=terminal&label" alt="fal Serverless" />
  </a>&nbsp;
  <a href="https://pepy.tech/project/dbt-fal">
    <img src="https://static.pepy.tech/personalized-badge/dbt-fal?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads" alt="Total downloads" />
  </a>&nbsp;
  <a href="https://pypi.org/project/dbt-fal/">
    <img src="https://badge.fury.io/py/dbt-fal.svg" alt="dbt-fal on PyPI" />
  </a>&nbsp;
  <a href="https://getdbt.slack.com/archives/C02V8QW3Q4Q">
    <img src="https://badgen.net/badge/icon/%23tools-fal%20on%20dbt%20Slack/orange?icon=slack&label" alt="Slack channel" />
  </a>&nbsp;
  <a href="https://discord.com/invite/Fyc9PwrccF">
    <img src="https://badgen.net/badge/icon/Join%20us%20on%20Discord/red?icon=discord&label" alt="Discord conversation" />
  </a>
</p>

# 🌅 Sunset Announcement

**Hey everyone!**

Just wanted to drop in and share some news: as of April 2024, we’re saying goodbye to dbt-fal. Yep, it’s been quite the ride, but we’re switching gears to pour all our energy into something super exciting – creating the first-ever generative media platform for developers over at [fal.ai](https://fal.ai)! 🚀 We’re all in on this and can’t wait to see where it takes us.

**Big thanks** to every single one of you who’s been with us on the dbt-fal adventure. Your support and contributions mean the world. We’ve done some awesome stuff together, and this isn’t the end. Just a new chapter. So, here’s to more amazing things ahead, and we’re stoked to have you join us for the ride.

Cheers!

## ❗ What Does This Mean?

- **No Further Development:** The project will no longer receive updates or new features.
- **Security Vulnerabilities:** We will not be addressing new security vulnerabilities after April 8, 2024. We advise users to consider this when deciding to continue the use of the project.
- **Archival:** The repository will be archived, making it read-only. While the code will remain accessible for educational and historical purposes, we encourage users to fork the repository if they wish to continue development on their own.

## 💬 FAQ

### Can I still use dbt-fal?
Yes, the project will remain available for use, but please be aware that no new updates or security patches will be provided moving forward.

### What are some alternatives to dbt-fal?
Unfortunately, none that we are aware of.

### I have more questions. Who can I talk to?
If you want to talk about dbt Python support, the best place to do so is the dbt Slack community. For other questions, reach out to hello@fal.ai

## 🙌 Special Thanks
We want to take a moment to thank everyone who contributed to dbt-fal, from our amazing contributors and users to anyone who spread the word about our project. Your support was invaluable.

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
