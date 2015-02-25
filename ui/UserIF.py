VENV_HOME = '/srv/lsm/.venv/rrcki-sendjob'
BIN_HOME = '/srv/lsm/rrcki-sendjob'
DATA_HOME = '/srv/lsm/data'

class UserIF:
    def __init__(self):
        print 'UserIF init'

    def getDataset(req, dataset, auth_key):
        from mq.MQ import MQ
        routing_key = 'method.getdataset'
        message = "%s&%s" % (dataset, auth_key)

        mq = MQ(host='localhost', exchange='lsm')
        mq.sendMessage(message, routing_key)

    def putDataset(req, file, dataset, auth_key):
        from mq.MQ import MQ
        routing_key = 'method.putdataset'
        message = "%s&%s&%s" % (file, dataset, auth_key)

        mq = MQ(host='localhost', exchange='lsm')
        mq.sendMessage(message, routing_key)

# Singleton
userIF = UserIF()
del UserIF

#Web interface methods
def getDataset(req, dataset, auth_key):
    userIF.getDataset(dataset, auth_key)

def putDataset(req, file, dataset, auth_key):
    userIF.getDataset(file, dataset, auth_key)
