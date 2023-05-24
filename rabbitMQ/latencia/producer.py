import pika
import time
import json
import random
from prometheus_client import start_http_server, Histogram, Gauge

class IoTDevice:
    def __init__(self, device_id, delta_t, data_size, messages_sent_counter, latency_histogram):
        self.device_id = device_id
        self.delta_t = delta_t
        self.data_size = data_size
        self.messages_sent_counter = messages_sent_counter
        self.latency_histogram = latency_histogram

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='iot_exchange', exchange_type='direct')
        self.channel.queue_declare(queue='iot_queue')
        self.channel.queue_bind(exchange='iot_exchange', queue='iot_queue', routing_key='data')

    def send_data(self):
        start_time = time.time()
        timestamp = start_time
        fractional = time.perf_counter() % 1
        timestamp_str = f"{timestamp:.0f}{fractional:.7f}"
        values = self.generate_random_data()
        data = {
            'Timestamp': timestamp_str,
            'Values': values
        }
        json_data = json.dumps(data)
        self.channel.basic_publish(exchange='iot_exchange',
                                   routing_key='data',
                                   body=json_data)
        end_time = time.time()
        latency = end_time - start_time
        self.messages_sent_counter.labels(device_id=str(self.device_id)).inc()  # Incrementar el contador de mensajes enviados
        self.latency_histogram.labels(device_id=str(self.device_id)).observe(latency)  # Observar la latencia del mensaje

        print(f"Device {self.device_id} - Data sent: {json_data}")
        print(f"Latency: {latency} seconds")

    def generate_random_data(self):
        value = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(self.data_size))
        return value

    def close_connection(self):
        if self.connection:
            self.connection.close()

# Configuración de los dispositivos IoT
num_devices = 1
delta_t = 2
data_size = 10

# Creación de métricas
messages_sent_counter = Gauge('iot_messages_sent_total', 'Total number of messages sent', ['device_id'])
latency_histogram = Histogram('iot_message_latency_seconds', 'Latency of messages', ['device_id'], buckets=[0.1, 0.5, 1, 2, 5])

# Iniciar servidor Prometheus
start_http_server(8000)  # Puerto 8000 para métricas Prometheus

# Creación y ejecución de los productores
devices = []
for i in range(num_devices):
    device = IoTDevice(i, delta_t, random.randint(1, data_size), messages_sent_counter, latency_histogram)
    device.connect()
    devices.append(device)

    for _ in range(100):  # Enviar 100 mensajes
        device.send_data()
        time.sleep(device.delta_t)

# Cierre de las conexiones
for device in devices:
    device.close_connection()
