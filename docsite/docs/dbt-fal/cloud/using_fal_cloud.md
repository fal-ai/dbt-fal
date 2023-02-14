---
sidebar_position: 1
---

# Using dbt-fal with fal cloud

`fal cloud` is our serverless compute solution that allows you to run Python models on a reliable and scalable infrastructure. Setting up dbt-fal with fal cloud is quick and straightforward.

## 0. Install fal

Skip this step, if you already have the latest version of fal installed,

```bash
pip install --upgrade fal[cloud]
```

## 1. Authenticate to fal cloud

fal cloud uses GitHub for authentication. Run the following command in your shell:

```bash
fal cloud login
```

Follow the link that's generated and login using GitHub. Come back to the shell, when ready.

## 2. Generate keys

Next, generate keys that will allow dbt to connect to fal cloud:

```bash
fal cloud generate-keys
```

This will print a message containing values for `KEY_ID` and `KEY_SECRET`. We will need these for setting up the dbt profile.

## 3. Update your dbt profiles.yml

In order to run your Python models in fal cloud, you should update the profiles.yml to include the newly generated credentials. Here's an example of how it should look like:

```yaml
fal_profile:
  target: fal_cloud
  outputs:
    fal_cloud:
      type: fal
      db_profile: db
      host: cloud
      key_secret: MY_KEY_SECRET_VALUE
      key_id: MY_KEY_ID_VALUE
    db:
      type: redshift
      host: MY_REDSHIFT_HOST
      port: MY_REDSHIFT_PORT
      ...
```

That's it. Doing a dbt run against this profile will execute your Python models in fal cloud.

## 4. (Optional) Define separate output for fal cloud

You can have fal outputs, e.g.:

```yaml
fal_profile:
  target: staging
  outputs:
    staging:
      type: fal
      db_profile: db
    prod:
      type: fal
      db_profile: db
      host: cloud
      key_secret: MY_KEY_SECRET_VALUE
      key_id: MY_KEY_ID_VALUE
    db:
      type: redshift
      host: MY_REDSHIFT_HOST
      port: MY_REDSHIFT_PORT
      ...
```

In the example above, we have two fal outputs: `staging` and `prod`. Output `staging` will execute your Python models only locally, whereas `prod` will run them on fal cloud. So now you can control where your models are ran with a `-t` flag.

This will run Python models locally:

```bash
dbt run
```

And this will run Python models on fal cloud:

```bash
dbt run -t prod
```
