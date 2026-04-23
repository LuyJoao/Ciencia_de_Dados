import firebase_admin
from firebase_admin import credentials, auth

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

usuario = auth.create_user(
    email="thiagoceron@gmail.com",
    password="abc12345"
)

usuario_encontrado = auth.get_user(usuario.uid)

print(usuario_encontrado.uid)
print(usuario_encontrado.email)
print(usuario_encontrado.display_name)