# coding: utf-8

from conf.env import TASK_DATA_HOME
from sql.hive_tag_export import HiveTagExport
from .user_tag import IGola_UserTagSql


class IGola_UserTagExportSql(HiveTagExport):

    _table_dst = IGola_UserTagSql._table_dst_tag
    _local_dir = TASK_DATA_HOME + "/igola_tag"