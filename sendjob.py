import commands
import time
from configobj import ConfigObj
from taskbuffer import JobSpec
from taskbuffer.FileSpec import FileSpec
from userinterface import Client

config = ConfigObj('job.conf')

class KIJobMaster:
    def __init__(self):
        self.jobList = []
        self.fileList = []

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
        s,o = Client.submitJobs(jobList)
        print "---------------------"
        print s
        for x in o:
            print "PandaID=%s" % x[0]

    def run(self):
        for job in self.jobList:
            jobs = []
            jobs.append(self.getBuildJob(job))
            #jobs.append(self.getStageInJob(job))
            jobs.append(self.getExecuteJob(job))
           # jobs.append(self.getStageOutJob(job))

            self.submitJobs(jobs)

master = KIJobMaster()

job = JobSpec()
job.jobDefinitionID = int(time.time()) % 10000
job.jobName = "user.ruslan.%s" % commands.getoutput('uuidgen')
job.transformation = '/s/ls2/home/users/poyda/bio/runbio_stageout_wr.py'
job.destinationDBlock = 'user.ruslan.test.%s' % commands.getoutput('uuidgen')
job.destinationSE = 'ANALY_RRC-KI-HPC'
job.currentPriority = 1000
job.prodSourceLabel = 'user'
job.computingSite = 'ANALY_RRC-KI-HPC'
job.cloud = 'RU'
job.prodDBlock = 'mc10_7TeV.105001.pythia_minbias.evgen.EVNT.e574_tid153937_00'
job.jobsetID = int(time.time())
master.jobList.append(job)

fileOT = FileSpec()
fileOT.lfn = ''
fileOT.destinationDBlock = 'user.ruslan.test.%s' % commands.getoutput('uuidgen')
fileOT.destinationSE = job.destinationSE
fileOT.dataset = job.destinationDBlock
fileOT.type = 'output'
fileOT.scope = 'panda'
fileOT.GUID = commands.getoutput('uuidgen')
master.fileList.append(fileOT)

master.run()
