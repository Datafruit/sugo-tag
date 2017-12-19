#!/bin/bash

if [ $# -ge 1 ]; then
    ts=$1
else
    ts=`date +%s`
fi

script_dir=$(dirname $0)
cd ${script_dir}/../

main="tag_server.py"
log="IGola_TagServer.log"

for pid in `ps -ef | grep ${main} | grep -v "grep" | awk '{print $2}'`
do
kill -9 ${pid}
done

python ${main} ${ts} >> ../log/${log} 2>&1
