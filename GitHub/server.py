import sys
import six

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer, KafkaConsumer
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
    "locked": False # Indica si el repositorio está bloqueado para 'push'
}

# Diccionario de réplicas
replicas = {
    "replica_1": repository_state.copy(),
    "replica_2": repository_state.copy(),
    "replica_3": repository_state.copy()
}

# Inicializar KafkaProducer para enviar respuestas
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def synchronize_replicas(replica_state, selected_replica):
    """Sincroniza todas las réplicas con la réplica seleccionada."""
    for replica in replicas:
        if replica != selected_replica:
            replicas[replica] = replica_state.copy()

def process_message(message):
    """Procesa un mensaje del cliente."""
    action = message['action']
    client = message['client']
    
    # Comprobar si el repository es None antes de intentar acceder a 'version'
    client_version = client.get('repository', {}).get('version', None) if client['repository'] else None
    username = client['username']
    
    # Simulación de un balanceador de carga
    selected_replica = random.choice(list(replicas.keys()))
    replica = replicas[selected_replica]
    
    # Lógica para manejar 'pull'
    if action == 'pull':
        if client['repository'] is None:
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
                "status": "Pull skipped: Already up-to-date.",
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
        if client['repository'] is None:
            response_message = {"username": username, "action": "push", "status": "Push denied: No repository found."}
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

    # Enviar la respuesta a través de Kafka
    producer.send(TOPIC_RESPONSES, response_message)

def main():
    # Inicializar KafkaConsumer para recibir solicitudes
    consumer = KafkaConsumer(TOPIC_REQUESTS, 
                             bootstrap_servers=KAFKA_SERVER, 
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print("Server is running and waiting for messages...")
    
    for message in consumer:
        process_message(message.value)

if __name__ == "__main__":
    main()
