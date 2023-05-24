import pika

class Consumer:
    def __init__(self, consumer_id):
        self.consumer_id = consumer_id
        self.connection = None
        self.channel = None

    def connect(self): #explicar
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='iot_exchange', exchange_type='direct')
        self.channel.queue_declare(queue='iot_queue')
        self.channel.queue_bind(exchange='iot_exchange', queue='iot_queue', routing_key='data')

    def process_data(self, ch, method, properties, body):
        print(f"Consumer {self.consumer_id} - Received data: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):#explicar
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='iot_queue', on_message_callback=self.process_data)
        self.channel.start_consuming()

    def close_connection(self):
        if self.connection:
            self.connection.close()

# Configuración de los consumidores
num_consumers = 3

# Creación y ejecución de los consumidores
consumers = []
for i in range(num_consumers):
    consumer = Consumer(i)
    consumer.connect()
    consumers.append(consumer)

    consumer.start_consuming()

# Cierre de las conexiones
for consumer in consumers:
    consumer.close_connection()
