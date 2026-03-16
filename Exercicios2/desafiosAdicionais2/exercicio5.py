import numpy as np

precos = np.array([120.50, 121.00, 119.80, 122.30, 120.00])

variacao = (precos[1:] - precos[:-1]) / precos[:-1] * 100

print("Preços:", precos)
print("Variação percentual diária (%):", variacao)