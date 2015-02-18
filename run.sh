#!/bin/bash

VENV_HOME=~/.venv/rrcki_sendjob
BIN_HOME=~/PycharmProjects/rrcki-sendjob
DATA_HOME=/tmp

#initial setup
source $VENV_HOME/bin/activate
#. $BIN_HOME/setup.sh

get(){
    dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK $1
    files=$(dq2-ls -f -p -L RRC-KI-T1_SCRATCHDISK $1 | grep 'srm://')
    echo $files
    for src in $files
    do
        IFS='/' read -ra ADDR <<< "$src"
        fname=${ADDR[${#ADDR[@]} - 1]}
        dest=$DATA_HOME/$1
        echo $dest/$fname
        python get.py $src $dest/$fname
        python ddm-put.py db $dest/$fname /$1/$fname
        rm $dest/$fname
    done
    rm -R $dest
}

put(){
    src=$1
    dataset=$2

    #get filename
    IFS='/' read -ra ADDR <<< "$src"
    fname=${ADDR[${#ADDR[@]} - 1]}
    twd=jXXXXXX
    cd $DATA_HOME
    wd=$(mktemp -d $twd)

    out=$(dq2-ls $dataset)
    if [[ $out == *"Data identifier not found."* ]];
    then
        #register dataset
        dq2-register-dataset $dataset
        dq2-register-location $dataset RRC-KI-T1_SCRATCHDISK
    fi

    #copy to local temp dir
    python $BIN_HOME/ddm-get.py db $src $wd/$fname

    #put to dataset
    python $BIN_HOME/put.py -t ATLASSCRATCHDISK db $wd/$fname $dataset
    rm -R $wd
}
put /df.py