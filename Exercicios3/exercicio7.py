import pandas as pd

experimento = pd.read_csv('experimento.csv', sep=',', decimal=',')

print(experimento.head())
print(experimento.tail())
print(experimento.describe())
