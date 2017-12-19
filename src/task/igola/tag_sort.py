# coding: utf-8

from conf.env import TASK_DATA_HOME
from data.csv import BaseDataCsv
from data.druid import BaseDataDruid
from sql.hive_tag import HiveTagSql
from sql.igola.user_tag_export import IGola_UserTagExportSql
from task.igola.tag_factory import igola_uindex_datasource_profile
from model.key_feature import KeyFeatureModel
from task.tag_sorting import TagSortingTask


class IGola_TagCsv(BaseDataCsv):
    _data_dir = TASK_DATA_HOME + "/sugo_tag_test"
    # _data_dir = IGola_UserTagExportSql._local_dir
    _fields_terminated = '\t'
    _col_all = [HiveTagSql._key, "feature", "value"]
    _col_key = [HiveTagSql._key]
    _col_feature = ["feature"]
    _col_value = ["value"]
    _pivot = True

class IGola_TagDruid(BaseDataDruid):
    _data_source = "sugo_profile_test"
    # _data_source = igola_uindex_datasource_profile
    _col_all = [HiveTagSql._key]
    _col_key = [HiveTagSql._key]
    _col_feature = []

class IGola_TaskTagSorting(TagSortingTask):
    _data_total = IGola_TagCsv()
    _user_group_cls = IGola_TagDruid
    _model = KeyFeatureModel
