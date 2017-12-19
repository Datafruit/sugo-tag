# coding: utf-8

import sys
import time
from luigi import IntParameter
from task.base import BaseTask, BaseTaskWithParams, BaseParameter


class Task1(BaseTask):
    def runner(self):
        for i in xrange(5):
            self.get_logger().info("############")
            time.sleep(1)

class Task2(BaseTask):
    def runner(self):
        for i in xrange(5):
            self.get_logger().info("############")
            time.sleep(1)
    def requires(self):
        return Task1()

class Task3(BaseTask):
    def runner(self):
        for i in xrange(5):
            self.get_logger().info("############")
            time.sleep(1)

class TaskP1(BaseTaskWithParams):
    def runner(self):
        for i in xrange(5):
            self.get_logger().info("############: %s", self._param.value)
            time.sleep(1)

class TaskP0(BaseTaskWithParams):
    def requires(self):
        return TaskP1('{"name": "%s", "value": "%s"}' % (self._param.name, self._param.value))


from task.druid_query_group import DruidQueryGroupTask
class TaskA(DruidQueryGroupTask):
    def runner(self):
        self.set_table_name("igola_profile")
        self.set_group_name("rkHxmfDCb")
        self.add_key("uid")
        for v in self.parse_data(self.get_group()):
            print v


from data.csv import BaseDataCsv
from data.druid import BaseDataDruid
from model.key_feature import KeyFeatureModel
from task.tag_sorting import TagSortingTask

class TestCsv(BaseDataCsv):
    _data_dir = "/Users/penghuan/Tmp/tag"
    _fields_terminated = '\t'
    _col_all = ["uid", "feature", "value"]
    _col_key = ["uid"]
    _col_feature = ["feature"]
    _col_value = ["value"]
    _pivot = True

class TestDruid(BaseDataDruid):
    _data_source = "igola_profile"
    _col_all = ["uid"]
    _col_key = ["uid"]
    _col_feature = []

class TestTagSortingTask(TagSortingTask):
    _data_total = TestCsv()
    _user_group_cls = TestDruid
    _model = KeyFeatureModel

class Task0(BaseTask):
    def requires(self):
        return Task2(), Task3()