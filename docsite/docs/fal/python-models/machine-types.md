---
sidebar_position: 4
---

# Machine Types

With fal-serverless, you can choose different sizes of machines to run each of your models. You are charged only for what you use.

To use different types of machines:

```python
$ vi models/orders_forecast.py

def model(dbt, fal):
    dbt.config(fal_environment="ml", fal_machine="GPU") # Add this line

    df: pd.DataFrame = dbt.ref("orders_daily")
```

The following options are available for the `fal_machine` argument:

| Value | Description                                      |
| ----- | ------------------------------------------------ |
| `XS`  | 0.25 CPU cores, 256 mb RAM (default)             |
| `S`   | 0.50 CPU cores, 1 gb RAM                         |
| `M`   | 2 CPU cores, 8 gb RAM                            |
| `L`   | 4 CPU cores, 32 gb RAM                           |
| `XL`  | 8 CPU cores, 128 gb RAM                          |
| `GPU` | 8 CPU cores, 64 gb RAM, 1 GPU core (a100, 40 gb) |
