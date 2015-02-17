#!/bin/bash
. setup.sh

files=$(dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK $1 | grep 'srm://')
echo $files
for src in $files
do
    IFS='/' read -ra ADDR <<< "$src"
    fname=${ADDR[${#ADDR[@]} - 1]}
    dest=/srv/lsm/data/$1/$fname
    echo $dest
    python rrcki-get.py $src $dest
done