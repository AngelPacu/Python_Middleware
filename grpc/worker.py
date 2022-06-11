

import pandas as dd


openDF = dict()

def read_csv(filepath):
    openDF[filepath] = dd.read_csv(filepath)
    print(openDF[filepath])
    return "Csv Read"

def maximum(filepath):
    return str(openDF.get(filepath).max())

    # Return the minimum of the values


def minimum(filepath):
    return str(openDF.get(filepath).min(axis=1))
