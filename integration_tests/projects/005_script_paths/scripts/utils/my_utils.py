import os
from functools import reduce


def write_data(data, model_name):
    temp_dir = os.environ["temp_dir"]

    write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".after.txt"]), "w")
    write_dir.write(data)
    write_dir.close()
