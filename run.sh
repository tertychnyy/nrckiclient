#!/bin/bash

VENV_HOME=/srv/lsm/.venv/rrcki-sendjob
BIN_HOME=/srv/lsm/rrcki-sendjob
DATA_HOME=/srv/lsm/data

#initial setup
source $VENV_HOME/bin/activate
. $BIN_HOME/setup.sh

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
        python ddm-put.py $dest/$fname /$1/$fname
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
    echo $out

    if [[ $out != *$dataset* ]];
    then
        #register dataset
        echo dq2-register-dataset $dataset
        echo dq2-register-location $dataset RRC-KI-T1_SCRATCHDISK
    fi

    #copy to local temp dir
    python $BIN_HOME/ddm-get.py db $src $DATA_HOME/$wd/$fname

    # /<site_prefix>/<space_token>/rucio/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>
    surl=srm://sdrm.t1.grid.kiae.ru:8443/srm/managerv2?SFN=/t1.grid.kiae.ru/data/atlas/atlasscratchdisk/

    #put to dataset
    echo python $BIN_HOME/put.py -t ATLASSCRATCHDISK db $DATA_HOME/$wd/$fname $dataset
    rm -R $DATA_HOME/$wd
}
#put /df.py user.ruslan.test.dataset
get panda.destDB.7af9eb41-4147-4ef6-908a-f930127840dc

#AdderAtlasPlugin._updateOutputs
#registerAPI = Register2.Register(tmpDest,force_backend=self.ddmBackEnd)
#out = registerAPI.registerFilesInDatasets(tmpIdMap)

#@param dataset: is a dictionary with a dataset name as the key and a list of its files, each being a dictionary.
#        {'dsn': [ 'guid', 'lfn', 'size', 'checksum', 'surl']}
