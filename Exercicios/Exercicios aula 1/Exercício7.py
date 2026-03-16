from collections import Counter

frase = "Três pratos de trigo para três tigres tristes"

palavras = frase.lower().split()

contador = Counter(palavras)

for palavra, qtd in contador.most_common(3):
    print(palavra, "->", qtd)