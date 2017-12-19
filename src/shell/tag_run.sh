#!/bin/bash

if [ $# -ge 1 ]; then
    ts=$1
else
    ts=`date +%s`
fi

script_dir=$(dirname $0)
cd ${script_dir}/../


sh shell/tag_factory_run.sh ${ts}
ret_code=$?
echo "tag_factory_run return code: ${ret_code}"
if [ ${ret_code} -ne 0 ]; then
    echo "tag_factory_run failed"
    exit ${ret_code}
fi
echo "tag_factory_run success"
sh shell/tag_server_run.sh ${ts} &
