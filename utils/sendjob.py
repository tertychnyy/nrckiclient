import commands
import os
import time
import sys
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from ui.Actions import moveData
from userinterface import Client
LOGFILE='/srv/lsm/log/sendjob.log'

def log(msg):
    try:
        f=open(LOGFILE, 'a')
        f.write("%s %s %s\n" % (time.ctime(), sessid, msg))
        f.close()
        os.chmod(LOGFILE, 0666)
    except:
        pass

def fail(errorcode=200,msg=None):
    if msg:
        msg='%s %s'%(errorcode, msg)
    else:
        msg=str(errorcode)
    print msg
    log(msg)
    sys.exit(errorcode)

class KIJobMaster:
    def __init__(self):
        self.jobList = []
        self.fileList = []

    def putData(self, params=None, fromSEparams=None, toSEparams=None):
        return moveData(params=params, fromSEparams=fromSEparams, toSEparams=toSEparams)

    def getData(self):
        #TODO
        pass

    def getBuildJob(self, injob):
        job = JobSpec()
        job.jobDefinitionID = int(time.time()) % 10000
        job.jobName = '%s.1' % injob.jobName
        job.transformation = '/s/ls2/home/users/poyda/bio-3/build.py'
        job.destinationDBlock = 'panda.destDB.%s' % commands.getoutput('uuidgen')
        job.destinationSE = injob.destinationSE
        job.currentPriority = 1000
        job.prodSourceLabel = injob.prodSourceLabel
        job.computingSite = injob.computingSite
        job.cloud = injob.cloud
        #job.jobsetID = injob.jobsetID
        job.jobParameters = ''

        fileOT = FileSpec()
        fileOT.lfn = '%s.lib.tgz' % job.jobName
        fileOT.destinationDBlock = '%s.1' % injob.destinationDBlock
        fileOT.destinationSE = job.destinationSE
        fileOT.dataset = job.destinationDBlock
        fileOT.type = 'output'
        fileOT.scope = 'panda'
        fileOT.GUID = commands.getoutput('uuidgen')
        job.addFile(fileOT)

        fileOL = FileSpec()
        fileOL.lfn = "%s.log.tgz" % job.jobName
        fileOL.destinationDBlock = job.destinationDBlock
        fileOL.destinationSE = job.destinationSE
        fileOL.dataset = job.destinationDBlock
        fileOL.type = 'log'
        fileOL.scope = 'panda'
        job.addFile(fileOL)

        return job

    def getStageInJob(self, injob):
        job = JobSpec()
        return job

    def getExecuteJob(self, injob):
        job = JobSpec()
        job.jobDefinitionID = int(time.time()) % 10000
        job.jobName = '%s.2' % injob.jobName
        job.transformation = '/s/ls2/home/users/poyda/bio-3/execute.py'
        job.destinationDBlock = 'panda.destDB.%s' % commands.getoutput('uuidgen')
        job.destinationSE = injob.destinationSE
        job.currentPriority = 1000
        job.prodSourceLabel = injob.prodSourceLabel
        job.computingSite = injob.computingSite
        job.cloud = injob.cloud
        #job.jobsetID = injob.jobsetID
        job.jobParameters = ''

        fileIT = FileSpec()
        fileIT.lfn = '%s.1.lib.tgz' % injob.jobName
        fileIT.dataset = '%s.1' % injob.destinationDBlock
        fileIT.prodDBlock = '%s.1' % injob.destinationDBlock
        fileIT.type = 'input'
        job.addFile(fileIT)

        for file in self.fileList:
            if file.type == 'input':
                continue

            if file.type == 'output':
                continue

        fileOT = FileSpec()
        fileOT.lfn = '%s.lib.tgz' % job.jobName
        fileOT.destinationDBlock = '%s.2' % injob.destinationDBlock
        fileOT.destinationSE = job.destinationSE
        fileOT.dataset = job.destinationDBlock
        fileOT.type = 'output'
        fileOT.scope = 'panda'
        fileOT.GUID = commands.getoutput('uuidgen')
        job.addFile(fileOT)

        fileOL = FileSpec()
        fileOL.lfn = "%s.log.tgz" % job.jobName
        fileOL.destinationDBlock = job.destinationDBlock
        fileOL.destinationSE = job.destinationSE
        fileOL.dataset = job.destinationDBlock
        fileOL.type = 'log'
        fileOL.scope = 'panda'
        job.addFile(fileOL)

        return job

    def getStageOutJob(self, injob):
        njob = JobSpec()
        return njob

    def submitJobs(self, jobList):
        print 'Submit jobs'
        s,o = Client.submitJobs(jobList)
        print "---------------------"
        print s
        for x in o:
            print "PandaID=%s" % x[0]


    def run(self):
        for job in self.jobList:
            jobs = []
            #jobs.append(self.getBuildJob(job))
            #jobs.append(self.getStageInJob(job))
            #jobs.append(self.getExecuteJob(job))
            #jobs.append(self.getStageOutJob(job))

        self.submitJobs(self.jobList)

if __name__ == '__main__':
    master = KIJobMaster()

    sessid="%s.%s" % ( int(time.time()), os.getpid())
    scope = 'user.ruslan'
    dblock = "%s.%s" % (scope, sessid)

    log(' '.join(sys.argv))

    args = sys.argv[1:]

    if len(args) < 6:
        fail(501, 'Incorrect number of arguments')
    trf = args[0]
    outfile = args[1]
    inputType = args[2]
    inputParam = args[3]
    outputType = args[4]
    outputParam = args[5]
    paramsList = args[6:]

    params = ' '.join(paramsList)

    if not trf.startswith('/s/ls/users/poyda'):
        fail(500, 'Illegal distr name')


    job = JobSpec()
    job.jobDefinitionID = int(time.time()) % 10000
    job.jobName = "%s.%s" % (scope, commands.getoutput('uuidgen'))
    job.transformation = trf
    job.destinationDBlock = 'panda.destDB.%s' % commands.getoutput('uuidgen')
    job.destinationSE = 'ANALY_RRC-KI-HPC'
    job.currentPriority = 1000
    job.prodSourceLabel = 'user'
    job.computingSite = 'ANALY_RRC-KI-HPC'
    job.cloud = 'RU'
    job.prodDBlock = dblock
    #job.jobsetID = int(time.time())

    job.jobParameters = params

    fileIT = FileSpec()
    fileIT.lfn = job.jobName + '.input.tgz'
    fileIT.dataset = job.prodDBlock
    fileIT.prodDBlock = job.prodDBlock
    fileIT.type = 'input'
    fileIT.scope = scope
    job.addFile(fileIT)

    fileOT = FileSpec()
    fileOT.lfn = outfile
    fileOT.destinationDBlock = job.prodDBlock
    fileOT.destinationSE = job.destinationSE
    fileOT.dataset = job.prodDBlock
    fileOT.type = 'output'
    fileOT.scope = scope
    fileOT.GUID = commands.getoutput('uuidgen')
    job.addFile(fileOT)


    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.type = 'log'
    fileOL.scope = 'panda'
    job.addFile(fileOL)

    fromSEparams = {'label': inputType,
                    'src': inputParam}
    toSEparams = {'label': 'grid',
                  'dest': fileIT.dataset}
    params = {'tmpdir': fileIT.dataset,
              'compress': True,
              'tgzname': fileIT.lfn}
    log('MoveDataTry')
    ec = master.putData(params=params, fromSEparams=fromSEparams, toSEparams=toSEparams)
    if ec!=0:
        fail(222, 'MoveDataError')
    log('MoveDataSuccess')
    master.jobList.append(job)
    log('Number of jobs: %s' % len(master.jobList))
    log('SendJobsTry')
    master.run()
    log('SendJobsSuccess')