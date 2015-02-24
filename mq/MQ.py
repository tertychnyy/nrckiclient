import pika
import threading

BIN_HOME = '/Users/it/PycharmProjects/rrcki-sendjob'

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
                                 type='topic')

        channel.basic_publish(exchange='lsm',
                              routing_key=routing_key,
                              body=message,
                              properties=pika.BasicProperties(
                                 delivery_mode = 2, # make message persistent
                              ))
        connection.close()

    def startConsumer(self, binding_keys):
        print 'startConsumer'
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        channel = connection.channel()

        channel.exchange_declare(exchange='lsm',
                                     type='topic')

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

            from ui.UserIF import UserIF
            userif = UserIF()
            userif.getDataset(dataset, auth_key)

            ch.basic_ack(delivery_tag = method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(callback, queue=queue_name)

        channel.start_consuming()

    def startConsumerThread(self, keys):
        t1 = threading.Thread(target=self.startConsumer, args=(keys))
        t1.start()
