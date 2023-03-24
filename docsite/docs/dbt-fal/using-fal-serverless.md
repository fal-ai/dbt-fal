---
sidebar_position: 1
---

# Use dbt-fal with fal-serverless

fal-serverless is our serverless compute solution that allows you to run Python models on a reliable and scalable infrastructure. Setting up dbt-fal with fal-serverless is quick and straightforward.

## 0. Install fal-serverless

```bash
pip install fal-serverless
```

## 1. Authenticate to fal-serverless

fal-serverless uses GitHub for authentication. Run the following command in your shell:

```bash
fal-serverless auth login
```

Follow the link that's generated and login using GitHub. Come back to the shell, when ready.

## 2. Generate keys

Next, generate keys that will allow dbt to connect to fal cloud:

```bash
fal-serverless key generate
```

This will print a message containing values for `KEY_ID` and `KEY_SECRET`. We will need these for setting up the dbt profile.

## 3. Update your dbt profiles.yml

In order to run your Python models in fal-serverless, you should update the profiles.yml to include the newly generated credentials. Here's an example of how it should look like:

```yaml
fal_profile:
  target: fal_serverless
  outputs:
    fal_serverless:
      type: fal
      db_profile: db
      host: cloud
      key_secret: MY_KEY_SECRET_VALUE
      key_id: MY_KEY_ID_VALUE
    db:
      type: postgres
      host: MY_PG_HOST
      port: MY_PG_PORT
      ...
```

That's it. Doing a dbt run against this profile will execute your Python models in fal-serverless.

## 4. (Optional) Define separate output for fal-serverless

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
      host: cloud ## ask your account exec
      key_secret: MY_KEY_SECRET_VALUE
      key_id: MY_KEY_ID_VALUE
    db:
      type: postgres
      host: MY_PG_HOST
      port: MY_PG_PORT
      ...
```

In the example above, we have two fal outputs: `staging` and `prod`. Output `staging` will execute your Python models only locally, whereas `prod` will run them on fal-serverless. So now you can control where your models are ran with a `-t` flag.

This will run Python models locally:

```bash
dbt run
```

And this will run Python models on fal-serverless:

```bash
dbt run -t prod
```
