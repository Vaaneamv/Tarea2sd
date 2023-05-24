import pika
import time
from prometheus_client import start_http_server, Counter, Histogram, Gauge

class Consumer:
    def __init__(self, consumer_id, messages_received_counter, latency_histogram, average_latency_gauge, messages_failed_counter):
        self.consumer_id = consumer_id
        self.messages_received_counter = messages_received_counter
        self.latency_histogram = latency_histogram
        self.average_latency_gauge = average_latency_gauge
        self.messages_failed_counter = messages_failed_counter

        self.total_latency = 0
        self.total_messages = 0

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='iot_exchange', exchange_type='direct')
        self.channel.queue_declare(queue='iot_queue')
        self.channel.queue_bind(exchange='iot_exchange', queue='iot_queue', routing_key='data')

    def process_data(self, ch, method, properties, body):
        try:
            print(f"Consumidor {self.consumer_id} - Datos recibidos: {body.decode()}")
            self.messages_received_counter.inc()  # Incrementar el contador de mensajes recibidos
            end_time = time.time()
            latency = end_time - self.start_time
            self.total_latency += latency
            self.total_messages += 1
            self.average_latency_gauge.set(self.total_latency / self.total_messages)
            self.latency_histogram.labels(consumer_id=str(self.consumer_id)).observe(latency)  # Observar la latencia del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Consumidor {self.consumer_id} - Error al procesar los datos: {e}")
            self.messages_failed_counter.inc()

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.start_time = time.time()  # Almacenar el tiempo de inicio
        self.channel.basic_consume(queue='iot_queue', on_message_callback=self.process_data)
        self.channel.start_consuming()

    def close_connection(self):
        if self.connection:
            self.connection.close()

# Configuración de los consumidores
num_consumers = 1

# Creación de métricas
messages_received_counter = Counter('iot_messages_received_total', 'Número total de mensajes recibidos')
latency_histogram = Histogram('iot_message_latency_seconds', 'Latencia de mensajes', ['consumer_id'], buckets=[0.1, 0.5, 1, 2, 5])
average_latency_gauge = Gauge('iot_average_latency_seconds', 'Latencia promedio de mensajes recibidos')
messages_failed_counter = Counter('iot_messages_failed_total', 'Número total de mensajes fallidos')

# Iniciar servidor Prometheus
start_http_server(8001)  # Puerto 8000 para métricas Prometheus

# Creación y ejecución de los consumidores
consumers = []
for i in range(num_consumers):
    consumer = Consumer(i, messages_received_counter, latency_histogram, average_latency_gauge, messages_failed_counter)
    consumer.connect()
    consumers.append(consumer)

    consumer.start_consuming()

# Cierre de las conexiones
for consumer in consumers:
    consumer.close_connection()
