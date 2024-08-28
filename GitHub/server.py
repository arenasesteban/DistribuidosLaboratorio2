from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import random

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
TOPIC_REQUESTS = 'repo-requests'
TOPIC_RESPONSES = 'repo-responses'

# Estado del repositorio
repository_state = {
    "name": "LaboratorioDistribuidos",
    "version": "1.1",
    "locked": False  # Indica si el repositorio está bloqueado para 'push'
}

replicas = {
    "replica_1": repository_state.copy(),
    "replica_2": repository_state.copy(),
    "replica_3": repository_state.copy()
}

# Configurar Producer para enviar respuestas
producer = Producer({'bootstrap.servers': KAFKA_SERVER})

# Sincronizar réplicas
def synchronize_replicas(replica_state, selected_replica):
    for replica in replicas:
        if replica != selected_replica:
            replicas[replica] = replica_state.copy()

# Lógica para manejar el 'push' y 'pull'
def process_message(message):
    try:
        data = json.loads(message.value().decode('utf-8'))
        action = data['action']
        client = data.get('client', {})
        client_repository = client.get('repository', None)
        client_version = client_repository.get('version', None) if client_repository else None
        username = client.get('username', 'unknown')

        print(f"Processing message from {username} with action {action}")

        # Simulación de un balanceador de carga
        selected_replica = random.choice(list(replicas.keys()))
        replica = replicas[selected_replica]

        # Lógica para manejar 'pull'
        if action == 'pull':
            if client_repository is None:
                response_message = {
                    "username": username,
                    "action": "pull",
                    "status": "Pull completed: Repository created. Version: {}".format(replica['version']),
                    "repository": replica
                }
            elif client_version == replica['version']:
                response_message = {
                    "username": username,
                    "action": "pull",
                    "status": "Pull skipped: Already up-to-date."
                }
            else:
                response_message = {
                    "username": username,
                    "action": "pull",
                    "status": "Pull completed: Updated to version {}".format(replica['version']),
                    "repository": replica
                }

        # Lógica para manejar 'push'
        elif action == 'push':
            if client_repository is None:
                response_message = {"username": username, "action": "push", "status": "Push denied: No repository found."}
            elif client_version is None:
                response_message = {"username": username, "action": "push", "status": "Push denied: No version provided."}
            elif client_version != replica['version']:
                response_message = {"username": username, "action": "push", "status": "Push denied: Version mismatch. Please pull the latest version."}
            elif replica['locked']:
                response_message = {"username": username, "action": "push", "status": "Push denied: Repository is locked."}
            else:
                # Actualizar el repositorio
                replica['locked'] = True
                replica['version'] = str(float(replica['version']) + 0.1)
                replica['locked'] = False
                response_message = {"username": username, "action": "push", "status": "Push accepted. New version: {}".format(replica['version'])}

                # Sincronizar todas las réplicas
                synchronize_replicas(replica, selected_replica)

        else:
            response_message = {"username": username, "action": action, "status": "Unknown action."}

        # Enviar la respuesta a través de Kafka
        producer.produce(TOPIC_RESPONSES, value=json.dumps(response_message).encode('utf-8'))
        producer.flush()

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except KeyError as e:
        print(f"Missing expected key in message: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Configurar el Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_REQUESTS])

# Procesar mensajes entrantes
while True:
    try:
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(message.error())
        else:
            # Log para depuración
            print(f"Message received: {message.value().decode('utf-8')}")
            process_message(message)

    except KafkaException as e:
        print(f"Kafka error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")