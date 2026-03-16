import numpy as np

notas = np.array([80, 90, 70])
pesos = np.array([0.3, 0.5, 0.2])

media = np.average(notas, weights=pesos)

print(media)