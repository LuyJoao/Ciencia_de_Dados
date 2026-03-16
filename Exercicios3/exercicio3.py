import pandas as pd

sist = pd.read_csv('log_sistema.csv', engine = 'python', skiprows=2, nrows=2)

print(sist)