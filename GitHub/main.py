# main.py
from kafka import KafkaProducer, KafkaConsumer
import json

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
TOPIC_REQUESTS = 'repo-requests'
TOPIC_RESPONSES = 'repo-responses'

def send_message(action, client, producer):
    # Construcción del mensaje
    request_message = {
        "action": action,
        "client": client
    }

    print(f"Sent {action} message for {client['username']} with version {client.get('repository', {}).get('version', 'None')}")
    producer.send(TOPIC_REQUESTS, request_message)
    producer.flush()

def receive_response(client, consumer):
    for message in consumer:
        response = json.loads(message.value.decode('utf-8'))
        print(f"Received response for {response['action']}: {response['status']}")

        # Si el pull o push fue aceptado, actualizar el cliente
        if response['action'] == 'pull' and "repository" in response:
            client['repository'] = response['repository']
        elif response['action'] == 'push' and response['status'].startswith("Push accepted"):
            client['repository'] = response['repository']
        break  # Salir después de la primera respuesta recibida

    return client

def main():
    # Configuración del productor y consumidor de Kafka
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, 
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer(TOPIC_RESPONSES, 
                             bootstrap_servers=KAFKA_SERVER, 
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    repository_name = "LaboratorioDistribuidos"

    # Simulación de clientes y operaciones
    clients = [
        {"username": "arenasesteban", "repository": None},
        {"username": "BryanSalgado", "repository": {"name": "LaboratorioDistribuidos", "version": "1.0"}},
        {"username": "TheWillyrex", "repository": {"name": "LaboratorioDistribuidos", "version": "1.1"}}
    ]

    actions = ["push", "pull"]

    # Enviar solicitudes y esperar respuestas
    for action in actions:
        for i, client in enumerate(clients):
            send_message(action, client, producer)
            clients[i] = receive_response(client, consumer)

if __name__ == '__main__':
    main()