# coding utf-8

from datetime import datetime
from conf.env import TASK_DATE
from conf.env import TASK_JAR_HOME
from sql.base import BaseSql

from conf.env import TASK_UDF_HOME
from conf.env import PYTHON_BIN


class HiveSql(BaseSql):

    _task_date = TASK_DATE
    _udf_home = TASK_UDF_HOME
    _jar_dir = TASK_JAR_HOME
    _python_bin = PYTHON_BIN
    _db_dst_prefix = "sugo_tag"
    _db_dst = "{prefix}_{day}".format(
        prefix=_db_dst_prefix,
        day=datetime.strptime(TASK_DATE, "%Y-%m-%d").strftime("%Y%m%d")
    )
    _args_base = [
        "add jar /opt/apps/hive_sugo/hcatalog/share/hcatalog/hive-hcatalog-core-2.1.0.jar;",
        "set mapreduce.job.queuename=root.default;",
        "set hive.exec.compress.output=false;"
        "set mapreduce.task.timeout=3600000;",
        "set mapred.max.split.size=256000000;",
        "set mapred.min.split.size.per.node=256000000;",
        "set mapred.min.split.size.per.rack=256000000;",
    ]

    @classmethod
    def get_sql_name(cls):
        return cls.__name__

    @classmethod
    def args_add(cls):
        return []

    @classmethod
    def get_args(cls):
        args = cls._args_base[:] + cls.args_add()
        args.append("set mapred.job.name=%s;" % cls.get_sql_name())
        return '\n'.join(args)
