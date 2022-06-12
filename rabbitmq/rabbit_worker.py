import pika, sys, os
import pandas as dd
import numpy

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='petitions', durable=True)

openDF = dict[str, dd.DataFrame]()


def read_csv(filepath):
    openDF[filepath] = dd.read_csv(filepath)
    return "CSV read"


# numpy.sum can be replaced.
def apply(filepath, function):
    return str(openDF.get(filepath).apply(function))


def columns(filepath):
    return str(openDF.get(filepath).columns)


# Averages with matching numbers, if there are 2 matching values, it will average the entire row. EX: LATD. MEAN
# can replaced.
def groupby(filepath, by):
    return str(openDF.get(filepath).groupby(openDF[filepath][by]).mean())


# Return a N elements (DEFAULT N=5)
def head(filepath, num=5):
    return str(openDF.get(filepath).head(num))

    # Test if cells contain values


def isin(filepath, values):
    return str(openDF.get(filepath).isin(values))

    # Iterate function.


def items(filepath):
    df_str = ''
    for label, value in openDF[filepath].items():
        df_str += f'label: {label}\n'
        df_str += f'content:\n {value}\n'

    return str(df_str)
    # Return the maximum of the values


def maximum(filepath):
    return str(openDF.get(filepath).max())

    # Return the minimum of the values


def minimum(filepath):
    return str(openDF.get(filepath).min(axis=1))


switch_functions = {
    'read_csv': read_csv,
    'apply': apply,
    'columns': columns,
    'groupby': groupby,
    'head': head,
    'isin': isin,
    'items': items,
    'maximum': maximum,
    'minimum': minimum
}


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
