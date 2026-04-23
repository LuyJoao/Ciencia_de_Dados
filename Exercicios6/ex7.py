import firebase_admin
from firebase_admin import credentials, auth
import firebase_admin.exceptions

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

def criar_usuario(email, senha):
    try:
        usuario = auth.create_user(email=email, password=senha)
        print(usuario.uid)
    except firebase_admin.exceptions.FirebaseError as e:
        print(f"Erro Firebase: {e.code} - {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")

criar_usuario("edularson@gmail.com", "abc12345")