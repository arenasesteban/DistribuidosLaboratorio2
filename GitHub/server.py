# server.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from kafka import KafkaProducer
import json

# Configuración de Spark
spark = SparkSession.builder.appName('GitHubConsistency').getOrCreate()

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

# Esquema para el DataFrame
schema = "action STRING, client STRUCT<username: STRING, repository: STRUCT<name: STRING, version: STRING>>"

# Lectura de los mensajes desde Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_REQUESTS) \
    .load()

# Conversión de los mensajes de Kafka a un DataFrame de Spark
requests_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = requests_df.withColumn("data", from_json(col("json_str"), schema)).select("data.*")

# Configurar KafkaProducer para enviar respuestas
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Lógica para manejar el 'push' y 'pull'
def process_row(row):
    action = row['action']
    client = row['client']
    client_version = client.get('repository', {}).get('version', None)
    username = client['username']
    
    
    # Lógica para manejar 'pull'
    if action == 'pull':
        if client['repository'] is None:
            response_message = {
                "username": username,
                "action": "pull",
                "status": "Pull completed: Repository created. Version: {}".format(repository_state['version']),
                "repository": repository_state
            }
        elif client_version == repository_state['version']:
            response_message = {
                "username": username,
                "action": "pull",
                "status": "Pull skipped: Already up-to-date.",
            }
        else:
            response_message = {
                "username": username,
                "action": "pull",
                "status": "Pull completed: Updated to version {}".format(repository_state['version']),
                "repository": repository_state
            }
    
    # Lógica para manejar 'push'
    elif action == 'push':
        if client['repository'] is None:
            response_message = {"username": username, "action": "push", "status": "Push denied: No repository found."}
        elif client_version != repository_state['version']:
            response_message = {"username": username, "action": "push", "status": "Push denied: Version mismatch. Please pull the latest version."}
        elif repository_state['locked']:
            response_message = {"username": username, "action": "push", "status": "Push denied: Repository is locked."}
        else:
            # Actualizar el repositorio
            repository_state['locked'] = True
            repository_state['version'] = str(float(repository_state['version']) + 0.1)
            repository_state['locked'] = False
            response_message = {"username": username, "action": "push", "status": "Push accepted. New version: {}".format(repository_state['version'])}

    # Enviar la respuesta a través de Kafka
    producer.send(TOPIC_RESPONSES, response_message)

# Aplicar la lógica a cada fila del DataFrame
query = parsed_df.writeStream.foreach(process_row).start()

# Esperar a que el streaming termine
query.awaitTermination()