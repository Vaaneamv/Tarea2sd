from kafka import KafkaProducer
from json import dumps
import time
import random
import statistics

def generate_random_data(data_size):
    value = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(data_size))
    return value

def main():
    servidores_bootstrap = 'kafka:9092'
    topic = 'mi_tema'
    delta_t = 0.5  # valor ajustado para reducir el tiempo de espera
    data_size = 10
    num_messages = 1000  # cantidad de mensajes a enviar

    producer = KafkaProducer(bootstrap_servers=[servidores_bootstrap],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             )

    latencies = []  # Lista para almacenar las latencias de envío

    for _ in range(num_messages):
        start_time = time.time()

        timestamp = time.time()
        fractional = time.perf_counter() % 1
        timestamp_str = f"{timestamp:.0f}{fractional:.7f}"

        values = generate_random_data(data_size)
        data = {
            'Timestamp': timestamp_str,
            'Values': values
        }

        producer.send(topic, value=data)
        producer.flush()

        end_time = time.time()
        latency = end_time - start_time
        latencies.append(latency)

        print(f"Enviando: {data}, Latencia: {latency}")

        time.sleep(delta_t)

    print("Estadísticas de latencia:")
    print(f"Promedio: {statistics.mean(latencies)}")
    print(f"Desviación estándar: {statistics.stdev(latencies)}")
    print(f"Mínimo: {min(latencies)}")
    print(f"Máximo: {max(latencies)}")

if __name__ == '__main__':
    main()