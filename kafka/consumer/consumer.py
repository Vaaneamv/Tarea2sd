from kafka import KafkaConsumer
from json import loads
import time
import statistics

servidores_bootstrap = 'kafka:9092'
topic = 'mi_tema'
group_id = 'mi_grupo'
num_messages = 1000  # cantidad de mensajes a consumir

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[servidores_bootstrap],
    auto_offset_reset='earliest',
    group_id=group_id
)

latencies = []  # Lista para almacenar las latencias de consumo

start_time = time.time()

for idx, msg in enumerate(consumer):
    end_time = time.time()
    latency = end_time - start_time
    latencies.append(latency)

    data = msg.value
    print(f"Grupo: {group_id}, Recibido: {data}, Latencia: {latency}")

    if idx + 1 >= num_messages:
        break

    start_time = time.time()

print("Estadísticas de latencia:")
print(f"Promedio: {statistics.mean(latencies)}")
print(f"Desviación estándar: {statistics.stdev(latencies)}")
print(f"Mínimo: {min(latencies)}")
print(f"Máximo: {max(latencies)}")
