
# GitHub Consistency System

Este proyecto simula un sistema de gestión de versiones para un repositorio utilizando Apache Kafka para la comunicación entre un servidor y clientes.

## Requisitos Previos

Antes de instalar y ejecutar este proyecto, asegúrate de tener instalado lo siguiente:

- **Python**: Versión 3.12 o superior
- **Apache Kafka**: Versión 2.13.0 o superior

## Instalación

Sigue los pasos a continuación para configurar y ejecutar el proyecto en tu entorno local.

### 1. Clona el repositorio

Clona el repositorio en uan carpeta que te acomode

### 2. Configura y ejecuta Apache Kafka

1. Descarga y descomprime Apache Kafka desde su [sitio oficial](https://kafka.apache.org/downloads).
2. Navega al directorio donde descomprimiste Kafka.
3. Inicia ZooKeeper (necesario para Kafka):

    ```bash
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

4. En otra terminal, inicia el broker de Kafka:

    ```bash
    bin/kafka-server-start.sh config/server.properties
    ```

5. Crea los tópicos necesarios para el proyecto:

    ```bash
    bin/kafka-topics.sh --create --topic repo-requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    bin/kafka-topics.sh --create --topic repo-responses --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```

### 3. Instala las dependencias de Python

Instala las dependencias utilizando `pip`:

```bash
pip install confluent-kafka==2.1.1
```

### 4. Ejecución del Proyecto

#### 1. Inicia el Servidor

En una terminal, ejecuta el servidor que estará esperando solicitudes de los clientes:

```bash
python server.py
```

#### 2. Ejecuta el Cliente

En otra terminal, ejecuta el script del cliente que enviará las solicitudes al servidor:

```bash
python main.py
```

### 5. Verificación de la Ejecución

Una vez ejecutados ambos scripts, podrás observar cómo los mensajes de `push` y `pull` son procesados por el servidor, y cómo los clientes reciben y procesan las respuestas.

## Versiones de Software Utilizadas

- **Python**: 3.12
- **Apache Kafka**: 2.13.0
- **confluent-kafka**: 2.5.0

## Notas

- Asegúrate de que el broker de Kafka y ZooKeeper están funcionando correctamente antes de ejecutar los scripts.
- Si tienes algún problema con la instalación o ejecución, verifica que las versiones de software sean las correctas y que todos los pasos hayan sido seguidos correctamente.
