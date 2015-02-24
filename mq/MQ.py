import pika

from ui.UserIF import UserIF

class MQ:
    def __init__(self):
        print 'MQ initialization'

    def getClient(self, server):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=server))
        channel = connection.channel()
        return channel, connection

    def sengMessage(self, message, routing_key):
        channel, connection = self.getClient('localhost')

        channel.exchange_declare(exchange='lsm',
                                 type='direct')

        channel.basic_publish(exchange='lsm',
                              routing_key=routing_key,
                              body=message,
                              properties=pika.BasicProperties(
                                 delivery_mode = 2, # make message persistent
                              ))
        connection.close()

    def startConsumer(self, binding_keys):
        channel, connection = self.getClient('localhost')

        channel.exchange_declare(exchange='lsm',
                                 type='direct')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        if not binding_keys:
            raise AttributeError('No keys for queue')

        for key in binding_keys:
            channel.queue_bind(exchange='lsm',
                               queue=queue_name,
                               routing_key=key)

        def callback(ch, method, properties, body):
            dataset, auth_key = body.split(' ')

            userif = UserIF()
            userif.getDataset(dataset, auth_key)

            ch.basic_ack(delivery_tag = method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)

        channel.start_consuming()