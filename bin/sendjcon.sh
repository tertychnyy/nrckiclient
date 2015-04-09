#!/bin/bash

. /home/apf/.pandatest_env
source /srv/webclient/bin/activate
export PANDA_URL=http://vcloud30.grid.kiae.ru:25080/server/panda
export PANDA_URL_SSL=https://vcloud30.grid.kiae.ru:25443/server/panda
export PYTHONPATH=/srv/panda/lib/python2.6/site-packages/pandacommon:/srv/panda/lib/python2.6/site-packages/pandaserver:/srv/nrckiclient/lib:/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/lib/:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/lib/python2.6/site-packages:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/externals/kerberos/lib.slc6-x86_64-2.6:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/externals/kerberos/lib.slc6-i686-2.6:$PYTHONPATH
export PATH=/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/bin/:$PATH

LSM_HOME=/srv/nrckiclient/lib

export X509_USER_PROXY=/tmp/x509up_u500
export X509_CERT_DIR=/etc/grid-security/certificates
export RUCIO_HOME=/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest
export RUCIO_AUTH_TYPE="x509_proxy"
export RUCIO_ACCOUNT=pilot

cd $LSM_HOME
python mq/consumer.py method.sendjob
