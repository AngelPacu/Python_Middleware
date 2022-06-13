import ast
from _ctypes_test import func

import pika, sys, os
import pandas as dd
import numpy

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='petitions', durable=True)



# numpy.sum can be replaced.
def apply(filepath, function):
    return str(dd.read_csv(filepath).apply(eval(function)))


def columns(filepath):
    return str(dd.read_csv(filepath).columns)


# Averages with matching numbers, if there are 2 matching values, it will average the entire row. EX: LATD. MEAN
# can replaced.
def groupby(filepath, by):
    return str(dd.read_csv(filepath).groupby(dd.read_csv(filepath)[by]).mean())


# Return a N elements (DEFAULT N=5)
def head(filepath, num=5):
    return str(dd.read_csv(filepath).head(num))

    # Test if cells contain values


def isin(filepath, values):
    return str(dd.read_csv(filepath).isin(values.split(",")))

    # Iterate function.


def items(filepath):
    df_str = ''
    for label, value in dd.read_csv(filepath).items():
        df_str += f'label: {label}\n'
        df_str += f'content:\n {value}\n'

    return str(df_str)
    # Return the maximum of the values


def maximum(filepath):
    return str(dd.read_csv(filepath).max(numeric_only='True'))

    # Return the minimum of the values


def minimum(filepath):
    return str(dd.read_csv(filepath).min(numeric_only='True'))


def attend_petition(body):
    petition = body.decode().split(",")
    print("I got " + petition[1])
    return eval(petition[1])(petition[0]) if petition[2] == '' else eval(petition[1])(petition[0], petition[2])


def send_petition(result, reply):
    channel.basic_publish(
        exchange='',
        routing_key=reply,
        body=result.encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        ))


def callback(ch, method, properties, body):
    result = attend_petition(body)
    send_petition(result, properties.reply_to)


channel.basic_consume(queue='petitions', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
