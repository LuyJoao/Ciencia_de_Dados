class Produto:
    
    def __init__(self, nome, preco, estoque):
        self.nome = nome
        self.preco = preco
        self.estoque = estoque

    def vender(self, quantidade):
        if quantidade <= self.estoque:
            self.estoque -= quantidade
            print(f"{quantidade} unidade(s) vendida(s).")
        else:
            print("Estoque insuficiente.")

    def repor(self, quantidade):
        self.estoque += quantidade
        print(f"{quantidade} unidade(s) adicionada(s) ao estoque.")

    def exibir_info(self):
        print("Produto:", self.nome)
        print("Preço: R$", self.preco)
        print("Estoque:", self.estoque)


produto1 = Produto("Mouse", 120.0, 10)

produto1.exibir_info()
produto1.vender(3)
produto1.repor(5)
produto1.exibir_info()