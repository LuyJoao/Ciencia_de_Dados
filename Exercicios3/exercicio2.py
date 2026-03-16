import pandas as pd

clima = pd.read_csv('clima.csv', parse_dates=['data'], index_col="data")

print(clima)

