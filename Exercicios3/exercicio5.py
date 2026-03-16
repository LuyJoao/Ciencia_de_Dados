import pandas as pd

transacoes = pd.read_csv('transacoes.csv', thousands= '.', decimal = ',')

print(transacoes)