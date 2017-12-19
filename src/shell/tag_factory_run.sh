#!/bin/bash

if [ $# -ge 1 ]; then
    ts=$1
else
    ts=`date +%s`
fi

script_dir=$(dirname $0)
cd ${script_dir}/../

## Luigi 参数说明(http://luigi.readthedocs.io/en/stable/configuration.html#core):
## [--workers]: worker 并发个数
## [--worker-timeout]: 每个 worker 的超时时间, 秒
## [--worker-keep-alive]: worker 执行完成后挂起等待同级别的 worker 执行完成
## [--worker-wait-interval]: worker 状态检查时间间隔
## [--scheduler-retry-count]: worker 失败后允许重试的最大次数
## [--scheduler-retry-delay]: worker 失败后允许多少秒后重试
## [--logging-conf-file]: 日志配置文件
## [--retcode-***]: 返回值
args_default=" \
    --local-scheduler \
    --workers 4 \
    --worker-timeout 3600 \
    --worker-keep-alive \
    --worker-wait-interval 5 \
    --scheduler-retry-count 1 \
    --scheduler-retry-delay 10 \
    --logging-conf-file conf/logging.cfg \
    --no-lock \
    --retcode-unhandled-exception 1 \
    --retcode-missing-data 2 \
    --retcode-task-failed 3 \
    --retcode-already-running 4 \
    --retcode-scheduling-error 5 \
    --retcode-not-run 6"

module_test="task.test"
module="task.igola.tag_factory"

task_test="Task0"
task="IGola_TaskEnd"

log="IGola_Luigi.log"

main="luigi_task.py"

# python ${main} ${ts} --module ${module_test} ${task_test} ${args_default}
# python ${main} ${ts} --module ${module} IGola_TaskUserValue ${args_default}
python ${main} ${ts} --module ${module} ${task} ${args_default} >> ../log/${log} 2>&1

