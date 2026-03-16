import numpy as np

#Crie um array NumPy 1D que contenha todos os números inteiros de 0 a 9.
array = np.arange(10)

print(array)

#Crie uma matriz 3x3 preenchida inteiramente com valores booleanos True.

matriz_bool = np.ones((3, 3), dtype=bool)

print(matriz_bool)

#Dado o array [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], extraia apenas os números ímpares.

impares = array[array % 2 != 0]

print(impares)

#No array [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], substitua todos os números ímpares por -1.

arr = np.arange(10)

arr[arr % 2 != 0] = -1

print(arr)

#Crie uma matriz 2D de forma 5x5 com números inteiros aleatórios entre 1 e 100.

matriz = np.random.randint(1, 100, size=(5, 5))

print(matriz)

#Usando a matriz aleatória do exercício anterior, calcule a soma de todos os elementos em cada coluna.

print(matriz.sum(axis=0))

#Encontre o valor máximo em cada linha da matriz aleatória criada no exercício 5.

print(matriz.max(axis=1))

#Dado um array a = np.array([1, 2, 3, 4, 5]), adicione o valor 2 a cada elemento usando broadcasting.

a  = np.array([1, 2, 3, 4, 5])

print(a + 2)

#Crie dois arrays 1D, a = np.array([1, 2, 3]) e b = np.array([4, 5, 6]), e concatene-os horizontalmente para formar um único array.

a = np.array([1, 2, 3])

b = np.array([4, 5, 6])

resultado = np.hstack((a, b))

print(resultado)

#Inverta a ordem dos elementos em um array 1D [10, 20, 30, 40], de forma que o resultado seja [40, 30, 20, 10].

array = [10, 20, 30, 40]

contra = np.flip(array)

print(contra)