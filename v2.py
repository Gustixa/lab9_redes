"""
Este módulo implementa una aplicación Streamlit para consumir y visualizar datos meteorológicos en tiempo real desde un broker Kafka.

Requiere las bibliotecas confluent_kafka, streamlit y matplotlib para el consumo de mensajes de Kafka y la visualización de datos.

Configuración:
- KAFKA_SERVER: Dirección del servidor Kafka.
- TOPIC: Tema desde el cual se consumirán los datos.

Funciones:
- decode_data(payload): Decodifica un payload de 3 bytes en temperatura, humedad y dirección del viento.
- plot_line_chart(data, label, ylabel): Crea un gráfico de línea para los datos dados.
- create_combined_temp_humidity_chart(temperaturas, humedades): Crea un gráfico combinado de temperatura y humedad.
- create_wind_direction_histogram(direcciones_viento): Crea un histograma de frecuencia de dirección del viento.
- main(): Consume datos de Kafka y actualiza las visualizaciones en Streamlit en un bucle infinito.
"""

import streamlit as st
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt

# Kafka configuration
KAFKA_SERVER = '164.92.76.15:9092'
TOPIC = '211024'

# Initialize Kafka Consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'grupo_consumidor',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

# Function to decode data from 3-byte payload
def decode_data(payload):
    """
    Decodifica un payload de 3 bytes en temperatura, humedad y dirección del viento.

    Args:
        payload (bytes): Payload codificado de 3 bytes que contiene los datos.

    Returns:
        tuple: Un tuple que contiene la temperatura (float), 
               la humedad (int) y la dirección del viento (str).
    """
    payload_int = int.from_bytes(payload, byteorder='big')
    temperatura_escalada = (payload_int >> 10) & 0x3FFF
    temperatura = (temperatura_escalada / 16383) * 110.0
    humedad = (payload_int >> 3) & 0x7F
    direccion_codificada = payload_int & 0x07
    direcciones = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    direccion_viento = direcciones[direccion_codificada]
    return temperatura, humedad, direccion_viento

# Streamlit app setup
st.title("Real-Time Weather Station Data")
st.subheader("Temperature, Humidity, and Wind Direction")

# Placeholders for real-time data
temperaturas = []
humedades = []
direcciones_viento = []

# Streamlit placeholders for plots
temperature_chart = st.empty()
humidity_chart = st.empty()
combined_chart = st.empty()
wind_direction_chart = st.empty()

# Function to create individual line plots with matplotlib
def plot_line_chart(data, label, ylabel):
    """
    Crea un gráfico de línea para los datos dados.

    Args:
        data (list): Lista de datos a graficar.
        label (str): Etiqueta para la línea del gráfico.
        ylabel (str): Etiqueta para el eje Y del gráfico.

    Returns:
        matplotlib.figure.Figure: La figura del gráfico de línea.
    """
    plt.clf()
    plt.plot(data, label=label)
    plt.xlabel("Data Points")
    plt.ylabel(ylabel)
    plt.legend()
    plt.tight_layout()
    return plt

# Function to create a combined temperature and humidity chart
def create_combined_temp_humidity_chart(temperaturas, humedades):
    """
    Crea un gráfico combinado de temperatura y humedad.

    Args:
        temperaturas (list): Lista de temperaturas a graficar.
        humedades (list): Lista de humedades a graficar.

    Returns:
        matplotlib.figure.Figure: La figura del gráfico combinado.
    """
    plt.clf()
    plt.plot(temperaturas, label='Temperature (°C)', color='tab:red')
    plt.plot(humedades, label='Humidity (%)', color='tab:blue')
    plt.xlabel("Data Points")
    plt.ylabel("Values")
    plt.legend()
    plt.title("Combined Temperature and Humidity")
    plt.tight_layout()
    return plt

# Function to create a wind direction bar chart
def create_wind_direction_histogram(direcciones_viento):
    """
    Crea un histograma de frecuencia de dirección del viento.

    Args:
        direcciones_viento (list): Lista de direcciones del viento.

    Returns:
        matplotlib.figure.Figure: La figura del histograma de direcciones del viento.
    """
    plt.clf()
    plt.hist(direcciones_viento, bins=8, edgecolor='black')
    plt.xlabel('Wind Direction')
    plt.ylabel('Frequency')
    plt.title("Wind Direction Frequency")
    plt.tight_layout()
    return plt

def main():
    """
    Consume datos de Kafka y actualiza las visualizaciones en Streamlit en un bucle infinito.

    En cada iteración, se decodifican los datos recibidos, se actualizan las listas de temperaturas, 
    humedades y direcciones del viento, y se grafican los resultados.
    """
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                st.error(f"Consumer error: {msg.error()}")
                break

        # Decode message
        datos_codificados = msg.value()
        temperatura, humedad, direccion_viento = decode_data(datos_codificados)
        st.write(f'Datos decodificados: Temperatura={temperatura}, Humedad={humedad}, Dirección={direccion_viento}')
        
        # Append data
        temperaturas.append(temperatura)
        humedades.append(humedad)
        direcciones_viento.append(direccion_viento)
        
        # Keep only the latest 50 entries for plotting
        if len(temperaturas) > 50:
            temperaturas.pop(0)
            humedades.pop(0)
            direcciones_viento.pop(0)
        
        # Update individual charts using matplotlib and Streamlit
        temperature_chart.pyplot(plot_line_chart(temperaturas, label='Temperature (°C)', ylabel='Temperature (°C)'))
        humidity_chart.pyplot(plot_line_chart(humedades, label='Humidity (%)', ylabel='Humidity (%)'))
        
        # Update combined temperature and humidity chart
        combined_chart.pyplot(create_combined_temp_humidity_chart(temperaturas, humedades))
        
        # Update wind direction bar chart with Streamlit
        wind_direction_chart.pyplot(create_wind_direction_histogram(direcciones_viento))

if __name__ == "__main__":
    main()
