# coding: utf-8

from conf.env import TASK_DATA_HOME
from data.csv import BaseDataCsv
from sql.hive_tag import HiveTagSql
from sql.igola.user_tag_export import IGola_UserTagExportSql
from model.recommend import RecommendModel
from task.tag_recommend import TagRecommendTask


class IGola_TagCsv(BaseDataCsv):
    _data_dir = TASK_DATA_HOME + "/sugo_tag_test"
    # _data_dir = IGola_UserTagExportSql._local_dir
    _fields_terminated = '\t'
    _col_all = [HiveTagSql._key, "feature", "value"]
    _col_key = [HiveTagSql._key]
    _col_feature = ["feature"]
    _col_value = ["value"]
    _pivot = True

class IGola_TaskTagRecommend(TagRecommendTask):
    _data_total = IGola_TagCsv()
    _model = RecommendModel
