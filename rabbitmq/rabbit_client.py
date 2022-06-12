import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='petitions', durable=True)

channel.queue_declare(queue='client', durable=True)


def send_petition(df, method, argument=''):
    message = df + ',' + method + ',' + argument
    channel.basic_publish(
        exchange='',
        routing_key='petitions',
        body=message.encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            reply_to='client'
        ))


def print_result(ch, method, properties, body):
    print(body.decode())


channel.basic_consume(queue='client', on_message_callback=print_result, auto_ack=True)

send_petition("../dataFiles/cities.csv", "read_csv")
send_petition("../dataFiles/cities.csv", "apply", "numpy.sum")
send_petition("../dataFiles/cities.csv", "read_csv")
send_petition("../dataFiles/cities.csv", "read_csv")
send_petition("../dataFiles/cities.csv", "read_csv")

channel.start_consuming()
