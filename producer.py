from pika import SelectConnection, URLParameters, BasicProperties, spec
from config import queue, host
from time import sleep

confirmed = errors = idx = 0


def on_open(conn):
    conn.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    global idx
    # channel.confirm_delivery(ack_nack_callback=on_delivery_confirmation)
    while True:
        message = f"Hello World! {idx}"
        channel.basic_publish(
            exchange='AlexTest',
            routing_key='',
            body=message,
            properties=BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        print(f" [x] Sent '{message}'")
        idx += 1
        # sleep(2)


def on_delivery_confirmation(frame):
    global confirmed, errors
    if isinstance(frame.method, spec.Basic.Ack):
        confirmed += 1
        print('Received confirmation: %r', frame.method)
    else:
        print('Received negative confirmation: %r', frame.method)
        errors += 1


if __name__ == '__main__':
    connection = SelectConnection(URLParameters(host), on_open_callback=on_open)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
