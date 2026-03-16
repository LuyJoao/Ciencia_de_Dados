import pandas as pd

for chunk in pd.read_csv('transacoes_grandes.csv', chunksize=20):
    print(chunk)
