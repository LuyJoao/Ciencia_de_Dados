def estatisticas(*args):
    media = sum(args) / len(args)
    maximo = max(args)
    minimo = min(args)

    return {
        "media": media,
        "maximo": maximo,
        "minimo": minimo
    }

resultado = estatisticas(10, 20, 30, 5, 15)

print(resultado)