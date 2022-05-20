# Example 6: Send model data to Google Firestore
Starting version 0.1.30, fal lets you easily send data to [Google Firestore](https://cloud.google.com/firestore/). This example assumes that Firestore is [already enabled](https://cloud.google.com/firestore/docs/quickstart-servers#create_a_in_native_mode_database) in your Google Cloud project.

## Credentials
By default fal will attempt to use the credentials provided in profiles.yml. If that's unsuccessful, credentials environment variable is checked. See [here](https://cloud.google.com/docs/authentication/getting-started) for more information on how to set such environment variable.

## Meta tag
In a `schema.yml` file, within a target model, a meta tag should be added in order to connect the model to fal:
```yaml
meta:
  fal:
    scripts:
      - path_to_fal_script.py
```

## `write_to_firestore` magic function
This function is available in fal runtime. There is no need to import it. Here's an example invocation:

```python
write_to_firestore(df=dataframe, collection=collection_name, key_column=key_column)
```

where `dataframe` refers to a pandas DataFrame to be sent to Firestore, `collection_name` is a desired collection in Firestore and key_column is the DataFrame column that will be used as document key. Note that every entry in `key_column` has to be unique.

## Fal script

Here's an example Python script that uses `write_to_firestore`:

```python
model_df = ref(context.current_model.name)
write_to_firestore(df=model_df, collection="zendesk_sentiment_data", key_column="id")
```

We first get a dbt model as a DataFrame `model_df` by using `ref` function and then we immediately send this data to Firestore, using the `id` column as a key. 

## Full example
You can find the full code example [here](https://github.com/fal-ai/fal_dbt_examples/blob/main/fal_scripts/write_to_firestore.py).
