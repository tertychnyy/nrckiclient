#!/usr/bin/env python
import pika
import sys
from ui.UserIF import UserIF

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='actions',
                         type='direct')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]

if not binding_keys:
    print >> sys.stderr, "Usage: %s [info] [warning] [error]" % \
                         (sys.argv[0],)
    sys.exit(1)

for key in binding_keys:
    channel.queue_bind(exchange='actions',
                       queue=queue_name,
                       routing_key=key)

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    dataset, auth_key = body.split(' ')

    userif = UserIF()
    userif.getDataset(dataset, auth_key)

    #тэг завершения работы
    ch.basic_ack(delivery_tag = method.delivery_tag)

#не выдавать новые задания пока не выполнено предыдущее
channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue_name)

channel.start_consuming()