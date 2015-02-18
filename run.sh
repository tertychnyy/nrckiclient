#!/bin/bash
source /srv/lsm/.venv/rrcki-sendjob/bin/activate
. /srv/lsm/rrcki-sendjob/setup.sh
dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK $1
files=$(dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK $1 | grep 'srm://')
echo $files
for src in $files
do
    IFS='/' read -ra ADDR <<< "$src"
    fname=${ADDR[${#ADDR[@]} - 1]}
    dest=/srv/lsm/data/$1/$fname
    echo $dest
    python get.py $src $dest
    python ddm-put.py $dest $dest
done