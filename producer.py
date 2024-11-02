"""
Este módulo simula el envío de datos de una estación meteorológica a un broker Kafka.

Requiere la biblioteca confluent_kafka para la producción de mensajes a Kafka.

Configuración:
- KAFKA_SERVER: Dirección del servidor Kafka.
- TOPIC: Tema donde se enviarán los datos.

Funciones:
- generar_datos(): Simula la generación de datos meteorológicos (temperatura, humedad y dirección del viento).
- encode_data(temperatura, humedad, direccion_viento): Codifica los datos de temperatura, humedad y dirección del viento en un payload de 3 bytes.
- enviar_datos(): Genera y envía datos codificados a Kafka en un bucle infinito.
"""

import random
import time
import numpy as np
from confluent_kafka import Producer

# Kafka configuration
KAFKA_SERVER = '164.92.76.15:9092'
TOPIC = '211024'

# Initialize Kafka Producer
producer_conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(producer_conf)

def generar_datos():
    """
    Simula la generación de datos de una estación meteorológica.

    Retorna:
        tuple: Un tuple que contiene la temperatura (float), 
               la humedad (int) y la dirección del viento (str).
    """
    temperatura = round(np.random.normal(55, 15), 2)  # Media 55°C, desviacion std  15
    humedad = random.randint(0, 100)  # Humedad entre 0 y 100%
    direccion_viento = random.choice(['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE'])  # Wind direction
    return temperatura, humedad, direccion_viento

def encode_data(temperatura, humedad, direccion_viento):
    """
    Codifica los datos de temperatura, humedad y dirección del viento en un payload de 3 bytes.

    Args:
        temperatura (float): Temperatura a codificar.
        humedad (int): Humedad a codificar.
        direccion_viento (str): Dirección del viento a codificar.

    Returns:
        bytes: Payload codificado de 3 bytes que representa los datos.
    """
    # Scale temperature to fit in 14 bits
    temperatura_escalada = int((temperatura / 110.0) * 16383) & 0x3FFF  # Mask with 0x3FFF to ensure 14 bits
    humedad_codificada = humedad & 0x7F  # Mask with 0x7F to ensure 7 bits
    direcciones = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    direccion_codificada = direcciones.index(direccion_viento) & 0x07  # Mask with 0x07 to ensure 3 bits

    # Combine all into a 24-bit payload
    payload = (temperatura_escalada << 10) | (humedad_codificada << 3) | direccion_codificada
    
    # Return as a 3-byte byte object
    return payload.to_bytes(3, byteorder='big')

def enviar_datos():
    """
    Genera y envía datos codificados a Kafka en un bucle infinito.

    En cada iteración, se generan nuevos datos de sensor, se codifican y se envían al tema especificado en Kafka.
    Se espera entre 15 y 30 segundos entre envíos.
    """
    while True:
        # Generate simulated sensor data
        temperatura, humedad, direccion_viento = generar_datos()
        
        # Encode data in 3 bytes
        datos_codificados = encode_data(temperatura, humedad, direccion_viento)
        
        print(f'Enviando datos codificados: {datos_codificados}')
        
        # Send encoded payload to Kafka
        producer.produce(TOPIC, value=datos_codificados)
        producer.flush()  # Ensure data is sent
        
        time.sleep(random.randint(15, 30))  # Wait between 15 and 30 seconds

if __name__ == "__main__":
    enviar_datos()
