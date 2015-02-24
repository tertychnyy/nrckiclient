#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='actions',
                         type='direct')
routing_key = sys.argv[1] if len(sys.argv) > 1 else 'method.unknown'
message = ' '.join(sys.argv[2:]) or ''

channel.basic_publish(exchange='actions',
                      routing_key=routing_key,
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
connection.close()