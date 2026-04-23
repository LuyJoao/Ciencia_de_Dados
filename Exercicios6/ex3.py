import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

def atualizar_preco(produto_id, novo_preco):
    ref = db.collection("produtos_mysql").document(produto_id)
    ref.set({
        "nome": "Mouse G PRO",
        "preco": novo_preco
    }, merge=True)

atualizar_preco("1", 500)