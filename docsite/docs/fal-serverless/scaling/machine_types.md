---
sidebar_position: 1
---

# Vertical - Machine Types and GPUs
You can specify the size of the machine that runs the target environment of an isolated function. This is done using the `machine_type` argument. The following options are available for the machine_type argument:

| Value    | Description                                         |
|----------|-----------------------------------------------------|
| `XS`     | 0.25 CPU cores, 256MB RAM (default)                 |
| `S`      | 0.50 CPU cores, 1GB RAM                             |
| `M`      | 2 CPU cores, 8GB RAM                                |
| `L`      | 4 CPU cores, 32GB RAM                               |
| `XL`     | 8 CPU cores, 128GB RAM                              |
| `GPU-T4` | 4 CPU cores, 26GB RAM, 1 GPU core (T4, 16 GB VRAM)  |
| `GPU`    | 8 CPU cores, 64GB RAM, 1 GPU core (A100, 40GB VRAM) |

For example:

```python
@isolated(machine_type="GPU")
def my_function():
  ...

@isolated(machine_type="L")
def my_other_function():
  ...
```

By default, the `machine_type` is set to `XS`.

You can also use an isolated function to define a new one with a different machine type:

```python
my_function_S = my_function.on(machine_type="S")
```

In the above example, `my_function_S` is a new isolated function that has the same contents as `my_function`, but it will run on a machine type `S`.

Both functions can be called:

```python
my_function() # executed on machine type `GPU`
my_function_S() # same as my_function but executed on machine type `S`
```

`my_function` is executed on machine type `GPU`. And `my_function_S`, which has the same logic as `my_function`, is now executed on machine type `S`.
