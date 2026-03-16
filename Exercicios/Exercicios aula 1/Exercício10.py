class Veiculo:
    
    def __init__(self, marca, modelo):
        self.marca = marca
        self.modelo = modelo

    def tipo_habilitacao(self):
        print("Tipo de habilitação desconhecido")


class Carro(Veiculo):

    def tipo_habilitacao(self):
        print("Habilitação necessária: Categoria B")


class Moto(Veiculo):

    def tipo_habilitacao(self):
        print("Habilitação necessária: Categoria A")


carro1 = Carro("Toyota", "Corolla")
moto1 = Moto("Honda", "CB250T")

print(carro1.marca, carro1.modelo)
carro1.tipo_habilitacao()

print(moto1.marca, moto1.modelo)
moto1.tipo_habilitacao()