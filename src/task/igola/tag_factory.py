# coding: utf-8

from datetime import datetime

from conf.env import TASK_DATE
from sql.hive_tag_update import HiveTagUpdateSql
from sql.igola.hive_prepare import IGola_HivePrepareSql
from sql.igola.user_behavior import IGola_UserBehaviorSql
from sql.igola.user_profile import IGola_UserProfileSql
from sql.igola.user_tag import IGola_UserTagSql
from sql.igola.user_value import IGola_UserValueSql
from sql.igola.user_value import IGola_UserValue2Sql
from sql.igola.user_value import IGola_UserValue3Sql
from sql.igola.user_value import IGola_UserValue4Sql
from sql.igola.user_tag_export import IGola_UserTagExportSql
from task.base import BaseTask
from task.hive import HiveTask
from task.hive_clean import HiveCleanTask
from task.uindex_ddl import UIndexDdlTask
from task.tag_update_check import TagUpdateCheckTask
from task.web_update import WebUpdateTask

day = datetime.strptime(TASK_DATE, "%Y-%m-%d").strftime("%Y%m%d")
igola_uindex_datasource_profile_prefix = "igola_profile"
igola_uindex_datasource_profile_tag_prefix = "igola_profile_tag"
igola_uindex_datasource_profile = "{prefix}_{day}".format(
    prefix=igola_uindex_datasource_profile_prefix,
    day=day
)
igola_uindex_datasource_profile_tag = "{prefix}_{day}".format(
    prefix=igola_uindex_datasource_profile_tag_prefix,
    day=day
)

class IGola_TaskUIndexCreateDst(UIndexDdlTask):
    def runner(self):
        self.set_table_name(igola_uindex_datasource_profile)
        self.add_key(IGola_UserTagSql._key, "string")
        for field_info in IGola_UserTagSql._fields:
            self.add_column(field_info["name"], field_info["type"])
        self.delete_history(igola_uindex_datasource_profile_prefix)
        self.delete()
        self.create()

class IGola_TaskUIndexCreateDict(UIndexDdlTask):
    def runner(self):
        self.set_table_name(igola_uindex_datasource_profile_tag)
        self.add_key("key", "string")
        self.add_column("name", "string")
        self.add_column("type", "string")
        self.add_column("tag_name", "string")
        self.add_column("tag_value", "string")
        self.delete_history(igola_uindex_datasource_profile_tag_prefix)
        self.delete()
        self.create()

class IGola_TaskUIndexPrepare(BaseTask):
    def requires(self):
        return IGola_TaskUIndexCreateDst(), IGola_TaskUIndexCreateDict()

class IGola_TaskHiveClean(HiveCleanTask):
    pass

class IGola_TaskHivePrepare(HiveTask):
    def get_sql(self):
        return IGola_HivePrepareSql()
    def requires(self):
        return IGola_TaskHiveClean()

class IGola_TaskBegin(BaseTask):
    def requires(self):
        return IGola_TaskHivePrepare(), IGola_TaskUIndexPrepare()

igola_task_begin = IGola_TaskBegin()

class IGola_TaskUserProfile(HiveTask):
    def get_sql(self):
        return IGola_UserProfileSql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserBehavior(HiveTask):
    def get_sql(self):
        return IGola_UserBehaviorSql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserValue(HiveTask):
    def get_sql(self):
        return IGola_UserValueSql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserValue2(HiveTask):
    def get_sql(self):
        return IGola_UserValue2Sql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserValue3(HiveTask):
    def get_sql(self):
        return IGola_UserValue3Sql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserValue4(HiveTask):
    def get_sql(self):
        return IGola_UserValue4Sql()
    def requires(self):
        return igola_task_begin

class IGola_TaskUserTag(HiveTask):
    def get_sql(self):
        return IGola_UserTagSql()
    def requires(self):
        return IGola_TaskUserProfile(), \
               IGola_TaskUserBehavior(), \
               IGola_TaskUserValue(), \
               IGola_TaskUserValue2(), \
               IGola_TaskUserValue3(), \
               IGola_TaskUserValue4()

class IGola_TaskUIndexUpdate(HiveTask):
    class IGola_HiveTagUpdateSql(HiveTagUpdateSql):
        _hive_tag_collector_sql_cls = IGola_UserTagSql
        _uindex_dst = igola_uindex_datasource_profile
        _uindex_dict = igola_uindex_datasource_profile_tag
    def get_sql(self):
        return self.IGola_HiveTagUpdateSql()
    def requires(self):
        return IGola_TaskUserTag()

class IGola_TaskTagUpdateCheck(TagUpdateCheckTask):
    _hive_task_log_file = IGola_TaskUIndexUpdate().get_log_file()
    _uindex_table_dst = igola_uindex_datasource_profile
    _uindex_table_dict = igola_uindex_datasource_profile_tag
    def requires(self):
        return IGola_TaskUIndexUpdate()

class IGola_TaskUserTagExport(HiveTask):
    def get_sql(self):
        return IGola_UserTagExportSql()
    def requires(self):
        return IGola_TaskTagUpdateCheck()

class IGola_TaskWebUpdateDimensions(WebUpdateTask):
    _uindex_datasource_prefix = igola_uindex_datasource_profile_prefix
    _uindex_datasource_profile = igola_uindex_datasource_profile
    _uindex_datasource_profile_tag = igola_uindex_datasource_profile_tag
    def requires(self):
        return IGola_TaskUserTagExport()
    def runner(self):
        self.update_dimensions()

class IGola_TaskEnd(BaseTask):
    def requires(self):
        return IGola_TaskWebUpdateDimensions()
