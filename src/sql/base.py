#coding: utf-8

from conf.env import TASK_SQL_HOME

class BaseSql(object):

    _task_sql_home = TASK_SQL_HOME

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def get_sql_name(cls):
        raise NotImplementedError

    @classmethod
    def get_sql_log_file(cls):
        return cls._task_sql_home + "/%s.sql" % cls.get_sql_name()

    @classmethod
    def get_sql(cls, *args, **kwargs):
        raise NotImplementedError
