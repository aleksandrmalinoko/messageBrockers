from pika import BlockingConnection, URLParameters
from config import queue, host

connection = BlockingConnection(URLParameters(host))
channel = connection.channel()

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(f" [x] Received {body}")


channel.basic_consume(queue, callback, auto_ack=True)
channel.start_consuming()