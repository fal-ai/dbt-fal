# Fal Feature Store

## What are we building?

A feature store is data system that facilitates managing the data transformations centrally for predictive analysis and ML models in production.

fal-dbt feature store is a feature store implementation that consists of a dbt package and a python library.

## Why are we doing this?

**Empower analytics engineer:** ML models and analytics operate on the same data. Analytics engineers know this data inside out. They are the ones setting up metrics, ensuring data quality and freshness. Why shouldn’t they be the ones responsible for the predictive analysis? With the rise of open source modelling libraries most of the work that goes into an ML model is done on the data processing side.

**Leverage the Warehouse:** Warehouses are secure, scalable and relatively cheap environments to do data transformation. Doing transformations in other environments is at least an order of magnitude more complicated. Warehouse should be part of the ML engineer toolkit especially for batch predictions. dbt is the best tool out there to do transformations with the warehouse. dbt feature store will make ML workflows leverage all the advantages of the modern data warehouses.
