# coding: utf-8

from conf.env import TASK_SQL_HOME
from task.hive import HiveTask
from sql.hive import HiveSql
from tools.command import Command


class HiveCleanTask(HiveTask):
    """
    just keep the latest N hive databases
    """

    _latest_n_db = 3
    _sql_dir = TASK_SQL_HOME

    def get_sql(self):
        pass

    def runner(self):
        db_prefix = HiveSql._db_dst_prefix
        sql_file_show_db = self._sql_dir + "/%sShowDb.sql" % self.get_task_name()
        sql_file_drop_db = self._sql_dir + "/%sDropDb.sql" % self.get_task_name()
        args = """
rm -f {sql_file_show_db} && rm -f {sql_file_drop_db} && \
    echo "show databases;" > {sql_file_show_db} && \
    {hive_bin} -f {sql_file_show_db} | \
    grep -E "{db_prefix}.+" | \
    sort -r | \
    awk 'BEGIN {{i=0}} {{i++; if(i>{latest_n_db}){{print $0}}}}' | \
    xargs -I {{}} echo "drop database if exists "{{}}" cascade;" > {sql_file_drop_db} && \
    {hive_bin} -f {sql_file_drop_db}
""".format(
            sql_file_show_db=sql_file_show_db,
            sql_file_drop_db=sql_file_drop_db,
            hive_bin=self._hive_bin,
            db_prefix=db_prefix,
            latest_n_db=self._latest_n_db
        )
        self.get_logger().info(args)
        Command.run(args, self.get_log_file())
