import hashlib
from ddm.DropboxSEPlugin import DropboxSEPlugin
from ddm.GridSEPlugin import GridSEPlugin

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,\
    FileAlreadyExists,Duplicate,DataIdentifierAlreadyExists

BIN_HOME='/srv/lsm/rrcki-sendjob'
SITE_PREFIX = 'srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN='
SITE_DATA_HOME = '/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/rucio'

class SEFactory:
    def __init__(self):
        print 'SEFactory initialization'

    def getSE(self, label, params=None):
        try:
            if label not in ['dropbox', 'grid']:
                raise AttributeError("Attribute 'label' error: Not found in list")

            if label == 'dropbox':
                se = DropboxSEPlugin(params)

            elif label == 'grid':
                se = GridSEPlugin(params)

            else:
                se = SEPlugin()
        except Exception:
            print Exception.message

        return se

class SEPlugin(object):
    def __init__(self, params=None):
        print 'SEPlugin initialization'

    def get(self, src, dest, fsize, fsum):
        raise NotImplementedError("SEPlugin.get not implemented")

    def put(self, src, dest):
        raise NotImplementedError("SEPlugin.put not implemented")

# rucio
class RucioAPI:
    # constructor
    def __init__(self):
        pass


    # extract scope
    def extract_scope(self,dsn):
        if ':' in dsn:
            return dsn.split(':')[:2]
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = ".".join(dsn.split('.')[0:2])
        return scope,dsn


    # register dataset with existing files
    def registerDatasetWithOldFiles(self,dsn,lfns=[],guids=[],sizes=[],checksums=[],lifetime=None):
        files = []
        for lfn, guid, size, checksum in zip(lfns, guids, sizes, checksums):
            if lfn.find(':') > -1:
                s, lfn = lfn.split(':')[0], lfn.split(':')[1]
            else:
                s = scope
            file = {'scope': s, 'name': lfn, 'bytes': size, 'meta': {'guid': guid}}
            if checksum.startswith('md5:'):
                file['md5'] = checksum[4:]
            elif checksum.startswith('ad:'):
                file['adler32'] = checksum[3:]
            files.append(file)
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            client.add_dataset(scope=scope, name=dsn)
            if lifetime != None:
                client.set_metadata(scope,dsn,key='lifetime',value=lifetime*86400)
        except DataIdentifierAlreadyExists:
            pass
        # open dataset just in case
        try:
            client.set_status(scope,dsn,open=True)
        except:
            pass
        # add files
        try:
            client.add_files_to_dataset(scope=scope,name=dsn,files=files, rse=None)
        except FileAlreadyExists:
            for f in files:
                try:
                    client.add_files_to_dataset(scope=scope, name=dsn, files=[f], rse=None)
                except FileAlreadyExists:
                    pass
        vuid = hashlib.md5(scope+':'+dsn).hexdigest()
        vuid = '%s-%s-%s-%s-%s' % (vuid[0:8], vuid[8:12], vuid[12:16], vuid[16:20], vuid[20:32])
        duid = vuid
        return {'duid': duid, 'version': 1, 'vuid': vuid}



    # register dataset location
    def registerDatasetLocation(self,dsn,rses,lifetime=None,owner=None):
        if lifetime != None:
            lifetime = lifetime*24*60*60
        scope,dsn = self.extract_scope(dsn)
        dids = []
        did = {'scope': scope, 'name': dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = '|'.join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner == None:
            owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule['rse_expression'] == location) and (rule['account'] == client.account):
                return True
        try:
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,weight=None,
                                        lifetime=lifetime, grouping='DATASET', account=owner,
                                        locked=False, notify='N',ignore_availability=True)
        except Duplicate:
            pass
        return True



    # register files in dataset
    def registerFilesInDataset(self,idMap):
        # loop over all rse
        attachmentList = []
        for rse,tmpMap in idMap.iteritems():
            # loop over all datasets
            for datasetName,fileList in tmpMap.iteritems():
                # extract scope from dataset
                scope,dsn = self.extract_scope(datasetName)
                files = []
                for tmpFile in fileList:
                    # extract scope from LFN if available
                    lfn = tmpFile['lfn']
                    if ':' in lfn:
                        s, lfn = lfn.split(':')
                    else:
                        s = scope
                    # set metadata
                    meta = {'guid': tmpFile['guid']}
                    if 'events' in tmpFile:
                        meta['events'] = tmpFile['events']
                    if 'lumiblocknr' in tmpFile:
                        meta['lumiblocknr'] = tmpFile['lumiblocknr']
                    # set mandatory fields
                    file = {'scope': s,
                            'name' : lfn,
                            'bytes': tmpFile['size'],
                            'meta' : meta}
                    checksum = tmpFile['checksum']
                    if checksum.startswith('md5:'):
                        file['md5'] = checksum[4:]
                    elif checksum.startswith('ad:'):
                        file['adler32'] = checksum[3:]
                    if 'surl' in tmpFile:
                        file['pfn'] = tmpFile['surl']
                    # append files
                    files.append(file)
                # add attachment
                attachment = {'scope':scope,
                              'name':dsn,
                              'dids':files}
                if rse != None:
                    attachment['rse'] = rse
                attachmentList.append(attachment)
        # add files
        client = RucioClient()
        return client.add_files_to_datasets(attachmentList,ignore_duplicate=True)



    # get disk usage at RSE
    def getRseUsage(self,rse,src='srm'):
        retMap = {}
        try:
            client = RucioClient()
            itr = client.get_rse_usage(rse)
            # look for srm
            for item in itr:
                if item['source'] == src:
                    try:
                        total = item['total']/1024/1024/1024
                    except:
                        total = None
                    try:
                        used = item['used']/1024/1024/1024
                    except:
                        used = None
                    try:
                        free = item['free']/1024/1024/1024
                    except:
                        free = None
                    retMap = {'total':total,
                              'used':used,
                              'free':free}
                    break
        except:
            pass
        return retMap



# instantiate
rucioAPI = RucioAPI()
del RucioAPI