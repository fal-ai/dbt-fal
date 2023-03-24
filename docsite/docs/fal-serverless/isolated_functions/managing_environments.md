---
sidebar_position: 2
---

# Managing Environments
The `@isolated` decorator supports two types of target environments: `virtualenv` and `conda`. To specify the type of environment to create, pass the `type` argument to the decorator. For example:

```python
@isolated(type="virtualenv")
def my_function():
  ...

@isolated(type="conda")
def my_other_function():
  ...
```
The default `type` is `virtualenv`, so `@isolated()` is the same as `@isolated(type="virtualenv")`

## `virtualenv` environments

When using a `virtualenv` environment, you can specify additional dependencies using the `requirements` argument, which takes a list of package names. For example:

```python
@isolated(type="virtualenv", requirements=["pyjokes"])
def my_function():
  ...
```

## `conda` environments

`conda` environmet allow users to define both system and Python packages. When using a `conda` environment, you dependencies can be specified by using the `packages` argument, which takes a list of package names. For example:

```python
@isolated(type="conda", packages=["pytorch"])
def my_other_function():
  ...
```

The conda environment has the conda-forge and pytorch channels enabled by default.

If you need more control over packages and channels, you can use either `env_yml` or `env_dict` argument instead of `packages`.

`env_yml` accepts a path to a YAML file with a [conda environment definition](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually). For example:

```python
@isolated(type="conda", env_yml="my_env.yml")
def my_other_function():
  ...
```

where `my_env.yml` could be something like:

```yaml
name: my_env
channels:
  - pytorch
  - defaults
dependencies:
  - pytorch
```

Similarly, `env_dict` is a dictionary representation of a conda environment YAML:

```python
my_env = {
  "name": "my_env",
  "channels": ["pytorch", "defaults"],
  "dependencies": ["pytorch"]
}

@isolated(type="conda", env_dict=my_env)
def my_other_function():
  ...
```
In the example above `my_env`  has the same structure as an environment YAML file. You can think of it as a parsed version of a conda environment YAML.
