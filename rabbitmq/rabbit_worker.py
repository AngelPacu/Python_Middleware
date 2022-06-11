import pika
import pandas as dd
import numpy

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='petitions', durable=True)

openDF = dict[str, dd.DataFrame]()


def read_csv(filepath, null):
    openDF[filepath] = dd.read_csv(filepath)
    print(openDF[filepath])
    return "CSV read"


# numpy.sum can be replaced.
def apply(filepath, function):
    return str(openDF.get(filepath).apply(function))


def columns(filepath, null):
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


def items(filepath, null):
    df_str = ''
    for label, value in openDF[filepath].items():
        df_str += f'label: {label}\n'
        df_str += f'content:\n {value}\n'

    return str(df_str)
    # Return the maximum of the values


def maximum(filepath, null):
    return str(openDF.get(filepath).max())

    # Return the minimum of the values


def minimum(filepath, null):
    return str(openDF.get(filepath).min(axis=1))


def attend_petition(ch, method, properties, body):
    petition = body.decode().split(",")
    result=petition[0](petition[1], petition[2])
    channel.queue_declare(queue=properties.reply_to)
    channel.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        body=result.encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        ))


channel.basic_consume(queue='petitions', on_message_callback=attend_petition, auto_ack=True)

channel.start_consuming()
