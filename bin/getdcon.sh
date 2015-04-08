#!/bin/bash
source /srv/lsm/.venv/rrcki-sendjob/bin/activate
export PYTHONPATH=/srv/pandaclient/lib:/cvmfs/atlas.cern.ch/repo/sw/ddm/latest/DQ2Clients/opt/dq2/lib/:/cvmfs/atlas.cern.ch/repo/sw/ddm/rucio-clients/latest/lib/python2.6/site-packages:$PYTHONPATH
LSM_HOME=/srv/pandaclient/lib

cd $LSM_HOME
python mq/consumer.py method.getdataset