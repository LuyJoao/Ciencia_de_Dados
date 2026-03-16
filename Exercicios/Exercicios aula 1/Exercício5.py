contatos = {
    "thiago" : "1234",
    "larson" : "0987",
    "maneiro" : "1324"
}

for nome, telefone in contatos.items():
    print(nome)
    
pesquisa = input("Digite o contato desejado: ")

if pesquisa in contatos:
    print(f"Telefone de : {pesquisa}:{contatos[pesquisa]}")
else:
    print("contato não encontrado")