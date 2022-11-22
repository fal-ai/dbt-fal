# How to migrate your after scripts to Python dbt models

Python dbt models (or Python Data Models) are a way to include a Python transformation of data inside of your dbt DAG.

We will explore how what we were able to achieve before with [`write_to_source`](../reference/variables-and-functions.md#write_to_source-function) and [`write_to_model`](../reference/variables-and-functions.md#write_to_model-function) in after scripts is now possible more clearly with Python dbt models.

## When to use a Python Data Model vs an after script

The rule-of-thumb is that if you are writing to the data warehouse, you should be using a Python Data Model.

We are deprecating the use of `write_to_source` and `write_to_model` outside of Python Data Models.

## Example

If you are already using `write_to_model` to enrich an existing table, you can remove said table and replace with the model.

Example commit: https://github.com/fal-ai/jaffle_shop_with_fal/commit/664620008679a3d18ba76b9f6421e9c908444bea
