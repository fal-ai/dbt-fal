# Isolated scripts (and environment management)

> Note: Isolated scripts feature is still experimental, and there might be both
> internal and external (user-visible) changes on how these environments are
> defined and managed.

If you have Fal Python models (or hooks) running up in your project with
dependencies against external packages (like `scikit-learn` for machine learning
models or `slack-sdk` for alerts ) you can let Fal manage such packages for you without
even thinking they are there. This provides extensive interoperability when
sharing this project with others (like your co-workers) and ensuring that the
local runs you are making are easily reproducible in different environments
(like in your CI/CD process).

## Defining environments

All the different environments that your Fal project might need can be defined
inside `fal_project.yml`, a file that is located in the same directory as your
`dbt_project.yml`, under your project's root directory.

An example `fal_project.yml` might look like this, where you have a list of
named environments:

```yml
environments:
  - name: alerts
    type: venv
    requirements:
      - slack-sdk==1.1.0

  - name: training
    type: conda
    packages:
      - scikit-learn=2.0.0
```

Each environment comes with a unique name (an identifier for you to reference it
later on, inside your `schema.yml`), a type (which would allow you to choose the
environment management backend that you want to use) and a list of configuration
options for the selected environment manager.

### Using virtual environments (for `pip` packages)

If your hook depends on packages from [PyPI](https://pypi.org/), you can create
an environment with the type `venv` and Fal will take care of creating and
managing (e.g. installing all the required packages) that virtual environment
for you.

The only parameter for `venv` based installations is the `requirements`
section, which is just a list of PyPI packages (and their pinned versions) that
your script needs. If you already have a `requirements.txt` laying around,
you can simply copy the contents from it.

```yml
environments:
  - name: interact-cloud
    type: venv
    requirements:
      - boto3==1.24
      - google-cloud==0.34
      - azure==5.0.0
```

The `interact-cloud` named environment above will have 3 dependencies (`boto3`,
`google-cloud` and `azure` libraries, as well as their dependencies) and each
hook/model that uses this environment can freely import any of the desired
functions that reside under these dependencies.

### Using conda-based solutions

If your program depends on packages from [Conda](https://conda.io/) (or if you
want to isolate the system level dependencies), you can use `conda` as the
environment management backend. It is very similar to `venv`, but this time
instead of defining a list of `requirements` you will define a list of
`packages` from the conda channels you have configured.

```yml
environments:
  - name: train
    type: conda
    packages:
      - fbprophet=0.7.1
      - scikit-learn=1.1.2
```

> Note: The environment above will be built with using the `conda` executable.
> If there aren't any executables named `conda` under your shell's binary search
> path, you can customize where Fal needs to look for the `conda` binary by
> specifying the `FAL_CONDA_HOME` environment variable. E.g. if you have a
> `miniconda` installation under `~/Downloads`, you can start fal with
> `FAL_CONDA_HOME=~/Downloads/miniconda3/bin fal flow run`.

Fal currently uses the global `~/.condarc` file when looking for channels to
install packages (or otherwise defaulting to the `conda` executable's bundled
search channels), so if you want to use packages from `conda-forge` (or any
other non-default channels), you can configure them with the following commands:

```console
$ conda config --add channels conda-forge
$ conda config --set channel_priority strict
```

## Specifying environments

You can specify environments for your hooks and models under your schema file
(`schema.yml`). There are currently two scopes to configure the environment
option, one is in the model-level (which all the hooks would automatically
inherit from) and the other one is in the hook-level (so only the selected hook
would run in the specified environment, and nothing else would be affected by
it).

### Changing a hook's environment

If you only want to run a hook in a specific environment, you can define the
`environment` attribute for that hook and Fal will ensure that the hook will be
executed under the specified environment.

```yml
version: 2

models:
  - name: reorder_alphabetically
    meta:
      fal:
        pre-hook:
          - print_enter_banner.py

        post-hook:
          - print_exit_banner.py
          - path: send_slack_alert.py
            environment: alerts
```

In the example above, both the Fal Python model `reoder_alphabetically` and the banner
printing scripts (`print_enter_banner.py` / `print_exit_banner.py`) will use
your computer's local environment (think of it as the Python environment Fal
itself is installed on) since they might not depend on any external Python
packages. But the `send_slack_alert.py` hook will use the `alerts` environment
that is defined under `fal_project.yml` which contains `slack-sdk` as a
dependency. So no matter where this runs (your computer, your co-worker's
computer or your project's CI environment), Fal will create a new virtual
environment and install all the necessary requirements that needs to be
available for `send_slack_alert.py` to work properly and a send a slack
notification.

### Changing a model's environment (with all the underlying hooks)

If you have a Fal Python model and want it to run on an isolated environment,
you can change the model-level environment which would then make Fal run your
Python model (as well as all the pre-hooks/post-hooks, unless you override it)
on the specified environment.

```yml
version: 2

models:
  - name: load_data_from_s3
    meta:
      fal:
        environment: interact-cloud
        pre-hook:
          - path: change_s3_permissions.py
            with:
              my_param: "you can still pas arbitrary parameters"

        post-hook:
          - path: change_s3_permissions.py
```

For the model above, both the Fal Python model `load_data_from_s3`
and the `change_s3_permissions.py` hooks will run on the same isolated
environment named `interact-cloud`. This is primarily useful when you want to
have a model-level environment that has everything that might be needed for the
specified model's execution (rather than fine-grained environments for each
hook/model script).

### Overriding model-level environment on hooks

If any of the hooks you have requires a different execution environment than
what is available as the model-level environment, you can simply change the
environment setting (as if setting it independently from the model) under the
model and Fal will make it run on its own environment (rather than the model
level one).

```yml
version: 2

models:
  - name: load_data_from_s3
    meta:
      fal:
        environment: interact-cloud
        pre-hook:
          - path: change_s3_permissions.py

        post-hook:
          - path: change_s3_permissions.py

          - path: send_slack_alert.py
            environment: alerts
```

If we add `send_slack_alert.py` to the example above, all the `s3` related
scripts will still run in the `interact-cloud` environment but the
`send_slack_alert.py` will now run under the `alerts` environment.

#### Overriding the model-level environment with the local environment

If you want your hooks to run in the same environment as your `fal` process
(without any sort of influence from the outside scopes, e.g. model-level
environments), you can use a reserved environment called `local`. It makes Fal
run them as if they are regular hooks without any environments, in the same
Python runtime that runs Fal itself.


```yml
version: 2

models:
  - name: load_data_from_s3
    meta:
      fal:
        environment: interact-cloud
        pre-hook:
          - path: print_enter_banner.py
            environment: local

        post-hook:
          - path: print_exit_banner.py
            environment: local
```

The `load_data_from_s3.py` (Fal model) will still continue to be ran inside
`interact-cloud` environment, but all the banner printing hooks now specify
`local` as their environment (which is a reserved name, so you can't re-define
it). Fal will run them as if it were running them outside of this model-level
environment's scope.


## Environment caches

Since creating an environment is the longest part of the job, Fal will avoid
doing so as long as there weren't any changes in the environment's definition.

```yml
environments:
  - name: alerts
    type: venv
    requirements:
      - slack-sdk==1.1.0

  - name: training
    type: conda
    packages:
      - scikit-learn=2.0.0
```

When the `alerts` or `training` environment is used for the first time, Fal will
create it from scratch and save it under your user cache dir (this depends on
your system, but you can see the default
[value in this page](https://github.com/platformdirs/platformdirs#example-output)).
For all the subsequent runs (of either the same script, or different ones that
use the same environment) Fal will try to use the already created environment
unless the environment definition itself has been changed. If the
`fal_project.yml` above evolves into something like the following (e.g. a new
dependency has been added under the `training` environment):

```yml
environments:
  - name: alerts
    type: venv
    requirements:
      - slack-sdk==1.1.0

  - name: training
    type: conda
    packages:
      - scikit-learn=2.0.0
      - xgboost=1.0.0
```

Fal will still use the same `alerts` environment no matter what, since it can
see that it hasn't been changed. But when the `training` environment is
referenced for the first time (after the change in the definition), it will now
re-create it as if it is creating a new environment and the same caching will
still be in effect (all the subsequent runs will now use the newly created
`training` environment, not the old one).

If two or more environments share the same set of unique dependencies
(`requirements` in `venv` based installations or `packages` in `conda` based
installations), they will point out to the same environment location under the
hood (if the definition for any of them changed, it won't affect the other ones,
due to the immutable nature of the created environments).
