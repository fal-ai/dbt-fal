from pandas import DataFrame

write_to_source(DataFrame({"a": [1, 2, 3]}), "results", "some_source")
