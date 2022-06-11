import pandas

import pandas as dd

df = dd.read_csv("dataFiles/cities.csv")
print(df)
print(df.max(axis="columns"))
