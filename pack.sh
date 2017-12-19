#!/bin/bash

if [ $# -ge 1 ]; then
    ts=$1
else
    ts=`date +%s`
fi

script_dir=$(dirname $0)
cd ${script_dir}/../
pack_file="sugo-tag_${ts}.tar.gz"
rm -f ${pack_file}
tar -czv \
    --exclude '*.log' \
    --exclude '*.pyc' \
    --exclude 'sugo-tag/data/algorithm_test/?*' \
    --exclude 'sugo-tag/data/igola_tag/?*' \
    --exclude 'sugo-tag/data/sugo_tag_test/?*' \
    --exclude 'sugo-tag/repo/igola/recommend/?*' \
    --exclude 'sugo-tag/repo/igola/sort/?*' \
    --exclude 'sugo-tag/task_sql/?*' \
    --exclude 'sugo-tag/task_tag/?*' \
    -f ${pack_file} sugo-tag

