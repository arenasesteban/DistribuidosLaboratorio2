# server.py
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
import json

# Configuración de Kafka
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'push-pull'

# Configuración de Spark
spark = SparkSession.builder.appName('GitHubConsistency').getOrCreate()

# Simulación del estado del repositorio
server_repository = {
    "name": "LaboratorioDistribuidos",
    "version": "1.1"
}

def process_message(message):
    action = message.get('action')
    client = message.get('client')
    client_version = client['repository']['version'] if client['repository'] else 'None'
    
    print(f"Processing {action} from user {client['username']} with version {client_version}")
    
    if action == 'push':
        # Crear un DataFrame en Spark con la información del cliente y del servidor
        data = [(client['username'], client_version, server_repository['version'])]
        columns = ["username", "client_version", "server_version"]
        df = spark.createDataFrame(data, columns)
        
        # Lógica de comparación de versiones con Spark
        df = df.withColumn("allowed", df.client_version >= df.server_version)
        
        result = df.collect()[0]
        
        if result['allowed']:
            print(f"Push allowed for {client['username']}")
            # Actualizar la versión del repositorio en el servidor
            server_repository['version'] = client['repository']['version']
        else:
            print(f"Push denied for {client['username']} - outdated version")
    
    elif action == 'pull':
        # Simulación de pull
        client['repository'] = server_repository
        print(f"Pull completed for {client['username']} with version {server_repository['version']}")
    
    return client

def main():
    # Configuración del consumidor y productor de Kafka
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for message in consumer:
        updated_client = process_message(message.value)
        producer.send(TOPIC, updated_client)

if __name__ == '__main__':
    main()