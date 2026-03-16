import numpy as np

numeros = np.arange(10)

pares = numeros[numeros % 2 == 0]

print("Array original:", numeros)
print("Números pares:", pares)