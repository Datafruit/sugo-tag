# coding: utf-8

from sql.hive import HiveSql


class IGola_HivePrepareSql(HiveSql):

    @classmethod
    def get_sql(cls):
        return """
{args};
drop database if exists {db_dst} cascade;
create database {db_dst};
""".format(
            args=cls.get_args(),
            db_dst=cls._db_dst
        )