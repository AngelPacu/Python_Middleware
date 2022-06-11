import pandas as dd

openDF = dict()

def read_csv(filepath):
    openDF[filepath] = dd.read_csv(filepath)
    print(openDF[filepath])
    return "CSV read"

    # Return the maximum of the values
def maximum(filepath,num):
    return str(openDF.get(filepath).max(num, numeric_only=True))


    # Return the minimum of the values
def minimum(filepath, num):
    return str(openDF.get(filepath).min(num, numeric_only=True))
