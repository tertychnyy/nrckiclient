import sys

class UserIF:
    def __init__(self):
        pass

    def sendJob(req, message):
        from mq.MQ import MQ
        routing_key = 'method.sendjob'

        mq = MQ(host='localhost', exchange='lsm')
        mq.sendMessage(message, routing_key)

    def getJobStatus(self, ids):
        import userinterface.Client as Client
        s,o = Client.getJobStatus(ids)
        result = {}
        if s != 0:
            return result
        for x in o:
            result[x.PandaID] = x.jobStatus
        return result
# Singleton
userIF = UserIF()
del UserIF

def senbJob(req, params):
    userIF.sendjob(params)

def getJobStatus(req, ids):
    userIF.getJobStatus(ids)