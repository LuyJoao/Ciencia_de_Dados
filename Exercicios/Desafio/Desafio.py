import json

class Livro:
    def __init__(self, titulo, autor, isbn, disponivel=True):
        self.titulo = titulo
        self.autor = autor
        self.isbn = isbn
        self.disponivel = disponivel

    def to_dict(self):
        return {
            "titulo": self.titulo,
            "autor": self.autor,
            "isbn": self.isbn,
            "disponivel": self.disponivel
        }

class Usuario:
    def __init__(self, nome, cpf):
        self.nome = nome
        self.cpf = cpf
        self.livros = []

    def to_dict(self):
        return {
            "nome": self.nome,
            "cpf": self.cpf,
            "livros": self.livros
        }


class Biblioteca:

    def __init__(self):
        self.livros = []
        self.usuarios = []

    def adicionar_livro(self, livro):
        self.livros.append(livro)

    def adicionar_usuario(self, usuario):
        self.usuarios.append(usuario)

    def listar_livros(self):
        for livro in self.livros:
            status = "Disponível" if livro.disponivel else "Emprestado"
            print(f"{livro.titulo} - {livro.autor} ({status})")

    def buscar_livro(self, titulo):
        for livro in self.livros:
            if livro.titulo.lower() == titulo.lower():
                return livro
        return None

    def buscar_usuario(self, cpf):
        for usuario in self.usuarios:
            if usuario.cpf == cpf:
                return usuario
        return None

    def emprestar_livro(self, titulo, cpf):
        livro = self.buscar_livro(titulo)
        usuario = self.buscar_usuario(cpf)

        if not livro:
            print("Livro não encontrado")
            return

        if not usuario:
            print("Usuário não encontrado")
            return

        if not livro.disponivel:
            print("Livro já emprestado")
            return

        livro.disponivel = False
        usuario.livros.append(livro.titulo)

        print("Livro emprestado com sucesso!")

    def devolver_livro(self, titulo, cpf):
        livro = self.buscar_livro(titulo)
        usuario = self.buscar_usuario(cpf)

        if livro and usuario and titulo in usuario.livros:
            livro.disponivel = True
            usuario.livros.remove(titulo)
            print("Livro devolvido!")
        else:
            print("Erro na devolução")

    def salvar_dados(self):

        dados = {
            "livros": [livro.to_dict() for livro in self.livros],
            "usuarios": [usuario.to_dict() for usuario in self.usuarios]
        }

        with open("biblioteca.json", "w") as f:
            json.dump(dados, f, indent=4)

    def carregar_dados(self):
        try:
            with open("biblioteca.json", "r") as f:
                dados = json.load(f)

            for l in dados["livros"]:
                self.livros.append(Livro(**l))

            for u in dados["usuarios"]:
                usuario = Usuario(u["nome"], u["cpf"])
                usuario.livros = u["livros"]
                self.usuarios.append(usuario)

        except:
            pass

biblioteca = Biblioteca()
biblioteca.carregar_dados()

while True:

    print("\n==== BIBLIOTECA ====")
    print("1 - Adicionar livro")
    print("2 - Cadastrar usuário")
    print("3 - Listar livros")
    print("4 - Emprestar livro")
    print("5 - Devolver livro")
    print("6 - Salvar")
    print("0 - Sair")

    opcao = input("Escolha: ")

    if opcao == "1":
        titulo = input("Título: ")
        autor = input("Autor: ")
        isbn = input("ISBN: ")

        biblioteca.adicionar_livro(Livro(titulo, autor, isbn))

    elif opcao == "2":
        nome = input("Nome: ")
        cpf = input("CPF: ")

        biblioteca.adicionar_usuario(Usuario(nome, cpf))

    elif opcao == "3":
        biblioteca.listar_livros()

    elif opcao == "4":
        titulo = input("Título do livro: ")
        cpf = input("CPF do usuário: ")

        biblioteca.emprestar_livro(titulo, cpf)

    elif opcao == "5":
        titulo = input("Título do livro: ")
        cpf = input("CPF do usuário: ")

        biblioteca.devolver_livro(titulo, cpf)

    elif opcao == "6":
        biblioteca.salvar_dados()

    elif opcao == "0":
        biblioteca.salvar_dados()
        print("Saindo...")
        break