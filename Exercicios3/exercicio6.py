import pandas as pd

sensores = pd.read_csv('sensores.csv', sep=',', na_values=['-', 'NA'])

print(sensores.info())