import pika
import time
import json
import random

class IoTDevice:
    def __init__(self, device_id, delta_t, data_size):
        self.device_id = device_id
        self.delta_t = delta_t
        self.data_size = data_size
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='iot_queue')

    def send_data(self):
        timestamp = time.time()
        fractional = time.perf_counter() % 1
        timestamp_str = f"{timestamp:.0f}{fractional:.7f}"
        values = self.generate_random_data()
        data = {
            'Timestamp': timestamp_str,
            'Values': values
        }
        json_data = json.dumps(data)
        self.channel.basic_publish(exchange='',
                                   routing_key='iot_queue',
                                   body=json_data)
        print(f"Device {self.device_id} - Data sent: {json_data}")

    def generate_random_data(self):
        value = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(self.data_size))
        return value

    def close_connection(self):
        if self.connection:
            self.connection.close()

# Configuración de los dispositivos IoT
num_devices = 5
delta_t = 2
data_size = 10

# Creación y ejecución de los productores
devices = []
for i in range(num_devices):
    device = IoTDevice(i, delta_t, random.randint(1, data_size))
    device.connect()
    devices.append(device)

    while True:
        device.send_data()
        time.sleep(device.delta_t)

# Cierre de las conexiones
for device in devices:
    device.close_connection()
