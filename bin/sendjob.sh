#!/bin/bash
. /home/apf/.pandatest_env
source /srv/webclient/bin/activate
export PYTHONPATH=/srv/panda/lib/python2.6/site-packages/pandaserver:/srv/panda/lib/python2.6/site-packages/pandacommon:/srv/lsm/rrcki-sendjob/:/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/lib/:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/lib/python2.6/site-packages:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/externals/kerberos/lib.slc6-x86_64-2.6:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/externals/kerberos/lib.slc6-i686-2.6:$PYTHONPATH
export PYTHONPATH=/srv/pandaclient/lib:$PYTHONPATH
export PATH=/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/bin/:$PATH
export X509_USER_PROXY=/tmp/x509up_u2502
export X509_CERT_DIR=/etc/grid-security/certificates
export RUCIO_HOME=/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest
export RUCIO_AUTH_TYPE="x509_proxy"
export RUCIO_ACCOUNT=pilot

LSM_HOME=/srv/pandaclient/lib
python /srv/pandaclient/bin/sendjob.py $@