import sys

class UserIF:
    def __init__(self):
        pass

    def sendJob(req, params):
        from mq.MQ import MQ
        routing_key = 'method.sendjob'
        message = '&'.join(params)

        mq = MQ(host='localhost', exchange='lsm')
        mq.sendMessage(message, routing_key)
# Singleton
userIF = UserIF()
del UserIF

def senbJob(req, params):
    userIF.sendjob(params)