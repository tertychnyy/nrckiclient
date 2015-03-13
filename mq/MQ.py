import pika
import threading
from ui.Actions import *
from utils import sendjob

BIN_HOME = '/Users/it/PycharmProjects/rrcki-sendjob'

class MQ:
    def __init__(self, host='localhost', exchange='default'):
        self.exchange = exchange
        self.host = host

    def getClient(self, server):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=server))
        channel = connection.channel()
        return channel, connection

    def sendMessage(self, message, routing_key):
        channel, connection = self.getClient(self.host)

        channel.exchange_declare(exchange=self.exchange,
                                 type='topic')

        channel.basic_publish(exchange=self.exchange,
                              routing_key=routing_key,
                              body=message,
                              properties=pika.BasicProperties(
                                 delivery_mode=2, # make message persistent
                              ))
        connection.close()

    def startGetDatasetConsumer(self):
        binding_keys = ['method.getdataset']

        channel, connection = self.getClient(self.host)

        channel.exchange_declare(exchange=self.exchange,
                                     type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        if not binding_keys:
            raise AttributeError('No keys for queue')

        for key in binding_keys:
            channel.queue_bind(exchange=self.exchange,
                               queue=queue_name,
                               routing_key=key)

        def callback(ch, method, properties, body):
            print body
            dataset, auth_key = body.split('&')

            getDataset(dataset, auth_key)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)

        channel.start_consuming()

    def startPutDatasetConsumer(self):
        binding_keys = ['method.putdataset']

        channel, connection = self.getClient(self.host)

        channel.exchange_declare(exchange=self.exchange,
                                     type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        if not binding_keys:
            raise AttributeError('No keys for queue')

        for key in binding_keys:
            channel.queue_bind(exchange=self.exchange,
                               queue=queue_name,
                               routing_key=key)

        def callback(ch, method, properties, body):
            print body
            file, dataset, auth_key = body.split('&')

            putDataset(file, dataset, auth_key)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)

        channel.start_consuming()

    def startSendJobConsumer(self):
        binding_keys = ['method.sendjob']

        channel, connection = self.getClient(self.host)

        channel.exchange_declare(exchange=self.exchange,
                                     type='topic')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        if not binding_keys:
            raise AttributeError('No keys for queue')

        for key in binding_keys:
            channel.queue_bind(exchange=self.exchange,
                               queue=queue_name,
                               routing_key=key)

        def callback(ch, method, properties, body):
            print body
            params = body.split('&')

            sendjob(params)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)

        channel.start_consuming()