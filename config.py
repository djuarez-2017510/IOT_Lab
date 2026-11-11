"""Parametros compartidos entre productor y consumidor."""
import os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "161.35.11.141:9092")

# credenciales que dio el catedratico para el laboratorio
KAFKA_AUTH = dict(
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("KAFKA_USER", "estudiante"),
    sasl_plain_password=os.getenv("KAFKA_PASS", "TryluRandomPass-2!@!"),
)

# topics: carne de uno de los integrantes
TOPIC_JSON = "23709"
TOPIC_COMPACT = "23709_compact"
SENSOR_KEY = "sensor1"
