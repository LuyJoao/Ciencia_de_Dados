import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

valor_minimo = 15.00

resultados = db.collection("produtos_mysql").where("preco", ">", valor_minimo).stream()

for doc in resultados:
    dados = doc.to_dict()
    print(dados.get("nome"), dados.get("preco"))