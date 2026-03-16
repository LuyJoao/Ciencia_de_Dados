import pandas as pd

for bloco in pd.read_csv('dados_sensor_gigante.csv', chunksize=10):
    media = bloco["temperatura"].mean()
    valores_ausentes = bloco["temperatura"].isna().sum
    print(bloco)
    print(media)
    print(valores_ausentes)
