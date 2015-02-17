#!/bin/bash
export LD_LIBRARY_PATH=/cvmfs/grid.cern.ch/emi-wn-3.7.3-1_sl6v2/usr/lib64/:/cvmfs/grid.cern.ch/emi-wn-3.7.3-1_sl6v2/lib64:$LD_LIBRARY_PATH
export PYTHONPATH=/cvmfs/grid.cern.ch/emi-wn-3.7.3-1_sl6v2/usr/lib64/python2.6/site-packages:/cvmfs/grid.cern.ch/emi-wn-3.7.3-1_sl6v2/usr/lib/python2.6/site-
export PATH=/cvmfs/grid.cern.ch/emi-wn-3.7.3-1_sl6v2/usr/bin/:/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/bin/:/cvmfs/atlas.cern.ch/repo/sw/dd
export RUCIO_HOME=/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest
export RUCIO_AUTH_TYPE="x509_proxy"
export X509_USER_PROXY=/tmp/x509up_u500
export X509_CERT_DIR=/etc/grid-security/certificates

. /home/apf/emi-wn_setup.sh
. /cvmfs/atlas.cern.ch/repo/sw/ddm/latest/setup.sh
export RUCIO_ACCOUNT=ruslan
. /cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/setup.sh
