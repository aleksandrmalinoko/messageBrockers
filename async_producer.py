# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205,W0603

import logging
import pika
from pika import spec, BasicProperties
from config import queue, host
from time import sleep

ITERATIONS = 10

logging.basicConfig(level=logging.INFO)

confirmed = 0
errors = 0
published = 0
msg_num = 0


def on_open(conn):
    conn.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    global published, msg_num
    channel.confirm_delivery(ack_nack_callback=on_delivery_confirmation)
    for _iteration in range(0, ITERATIONS):
        channel.basic_publish(
            exchange='AlexTest',
            routing_key='',
            body=f"message {msg_num}",
            properties=BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        published += 1
        msg_num += 1


def on_delivery_confirmation(frame):
    global confirmed, errors
    if isinstance(frame.method, spec.Basic.Ack):
        confirmed += 1
        logging.info('Received confirmation: %r', frame.method)
        logging.info(f"Now {confirmed} conf; {errors} err")
    else:
        logging.error('Received negative confirmation: %r', frame.method)
        errors += 1
    if (confirmed + errors) == ITERATIONS:
        logging.info(
            'All confirmations received, published %i, confirmed %i with %i errors',
            published, confirmed, errors)
        connection.close()


parameters = pika.URLParameters(f'{host}')  # ?connection_attempts=50')
connection = pika.SelectConnection(
    parameters=parameters,
    on_open_callback=on_open
)

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    connection.close()
    connection.ioloop.start()
