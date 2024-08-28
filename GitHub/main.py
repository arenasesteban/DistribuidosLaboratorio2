from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import json

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
TOPIC_REQUESTS = 'repo-requests'
TOPIC_RESPONSES = 'repo-responses'

def send_message(action, client, producer):
    # Construcción del mensaje
    request_message = json.dumps({
        "action": action,
        "client": client
    })

    # Verifica si 'repository' es None antes de intentar acceder a su contenido
    repository_version = client.get('repository', {}).get('version', 'None') if client.get('repository') else 'None'
    print(f"Sent {action} message for {client['username']} with version {repository_version}")

    producer.produce(TOPIC_REQUESTS, value=request_message)
    producer.flush()

def receive_response(client, consumer):
    while True:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(message.error())
        else:
            response = json.loads(message.value().decode('utf-8'))
            print(f"Received response for {response['username']}: {response['action']} - {response['status']}")

            if response['username'] == client['username']:
                # Actualizar el cliente basado en la respuesta recibida
                if response['action'] == 'pull' and "repository" in response:
                    client['repository'] = response['repository']
                elif response['action'] == 'push' and response['status'].startswith("Push accepted"):
                    if "repository" in response:  # Verificar si la clave 'repository' existe en la respuesta
                        client['repository'] = response['repository']
                break  # Salir después de la primera respuesta recibida

    return client

def main():
    # Configuración del productor y consumidor de Kafka
    producer = Producer({'bootstrap.servers': KAFKA_SERVER})
    consumer = Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([TOPIC_RESPONSES])  # Mover la suscripción aquí

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

    consumer.close()

if __name__ == '__main__':
    main()