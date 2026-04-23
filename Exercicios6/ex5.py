import os

def simular_upload(nome_arquivo, conteudo, bucket):
    with open(nome_arquivo, "w") as f:
        f.write(conteudo)
    print(f"Arquivo '{nome_arquivo}' criado localmente.")
    print(f"Simulando upload para bucket: {bucket}")
    print(f"URL simulada: https://storage.googleapis.com/{bucket}/{nome_arquivo}")
    os.remove(nome_arquivo)

simular_upload("meu_arquivo.txt", "Olá, Firebase Storage!", "cienciadados-12940.appspot.com")