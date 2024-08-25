# main.py
from kafka import KafkaProducer
import json

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'push-pull'

def send_message(action, client):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Construcción del mensaje
    message = {
        "action": action,
        "client": client
    }

    producer.send(TOPIC, message)
    print(f"Sent {action} message for {client['username']} with version {client['repository'].get('version', 'None')}")

def main():
    repository_name = "LaboratorioDistribuidos"

    # Cliente que aún no tiene el repositorio en su máquina
    clientA = {
        "username": "arenasesteban",
        "repository": None
    }

    # Cliente con una versión anterior del repositorio en su máquina
    clientB = {
        "username": "BryanSalgado",
        "repository": {
            "name": repository_name,
            "version": "1.0"
        }
    }

    # Cliente con la última versión del repositorio en su máquina
    clientC = {
        "username": "TheWillyrex",
        "repository": {
            "name": repository_name,
            "version": "1.1"
        }
    }

    send_message("push", clientA)
    send_message("push", clientB)
    send_message("push", clientC)

    send_message("pull", clientA)
    send_message("pull", clientB)
    send_message("pull", clientC)

if __name__ == '__main__':
    main()