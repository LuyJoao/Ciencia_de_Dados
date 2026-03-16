import numpy as np

matriz = np.random.randint(100, 500, size=(3, 4))

print("matriz aleatoria:\n", matriz)

print("venda total por semana:\n",matriz.sum(axis=1))

print("media de vendas de cada dia da semana:\n", matriz.mean(axis=0))

print("dias que tiveram mais de 400 de venda:\n", matriz[matriz > 400])