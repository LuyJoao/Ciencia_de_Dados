import pandas as pd

estoque = pd.read_csv('estoque.csv', sep=';', decimal=',')

print(estoque.dtypes)