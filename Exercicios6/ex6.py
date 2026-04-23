import firebase_admin
from firebase_admin import credentials, messaging

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

mensagem = messaging.Message(
    notification=messaging.Notification(
        title="Notificação de Teste",
        body="Olá, esta é uma notificação do Firebase Cloud Messaging."
    ),
    topic="geral"
)

resposta = messaging.send(mensagem)

print(resposta)