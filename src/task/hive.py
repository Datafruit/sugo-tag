#coding: utf-8

from conf.env import HIVE_BIN
from tools.command import Command
from task.base import BaseTask

class HiveTask(BaseTask):

    _hive_bin = HIVE_BIN

    def get_sql(self):
        raise NotImplementedError

    def runner(self):
        sql_obj = self.get_sql()
        sql_file = sql_obj.get_sql_log_file()
        with open(sql_file, 'w') as fd:
            print >> fd, sql_obj.get_sql()

        args = "cat %s" % sql_file
        # args="{hive_bin} -f {sql_file}".format(hive_bin=self._hive_bin, sql_file=sql_file)
        self.get_logger().info(args)
        Command.run(args, self.get_log_file())
