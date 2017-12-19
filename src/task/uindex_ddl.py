# coding: utf-8

import re
import time
import json

from conf.env import UINDEX_URL_CREATE
from conf.env import UINDEX_URL_DELETE
from conf.env import UINDEX_URL_SHOW_TABLES
from conf.env import DRUID_URL_PRETTY
from tools.http_method import get, post, delete
from task.base import BaseTask


class UIndexDdlTask(BaseTask):

    _uindex_url_create = UINDEX_URL_CREATE
    _uindex_url_delete = UINDEX_URL_DELETE
    _uindex_url_show_tables = UINDEX_URL_SHOW_TABLES
    _druid_url_pretty = DRUID_URL_PRETTY
    _latest_n_table = 3

    def __init__(self):
        super(UIndexDdlTask, self).__init__()
        self._body = {
            "datasourceName": None,
            "partitions": 1,
            "columnDelimiter": "|",
            "shardSpec": {
                "type": "single",
                "dimension": None
            },
            "dimensions": []
        }

    def set_table_name(self, name):
        self._body["datasourceName"] = name

    def add_column(self, name, type):
        self._body["dimensions"].append(
            {"name": name, "type": type}
        )

    def add_key(self, name, type):
        self._body["shardSpec"]["type"] = "single"
        self._body["shardSpec"]["dimension"] = name
        self.add_column(name, type)

    def create(self):
        url = self._uindex_url_create
        content_type = "application/json; charset=utf-8"
        data_json = json.dumps(self._body)
        self.get_logger().info("%s\n%s\n%s" % (url, content_type, data_json))
        res_code, res_data = post(url, data_json, content_type)
        if res_code != 200:
            self.get_logger().error("error code: %d, error info: %s" % (res_code, res_data))
            raise Exception("error code: %d, error info: %s" % (res_code, res_data))

        for i in xrange(120):
            if self.table_exists(self._body["datasourceName"]):
                time.sleep(60)
                return
            else:
                self.get_logger().info("waiting for creating [%s]" % self._body["datasourceName"])
                time.sleep(5)
        self.get_logger().error("failed to create [%s]" % self._body["datasourceName"])
        raise Exception("failed to create [%s]" % self._body["datasourceName"])

    def delete(self):
        url = "%s/%s" % (self._uindex_url_delete, self._body["datasourceName"])
        self.get_logger().info(url)
        res_code, res_data = delete(url)
        if res_code != 204 and res_code != 200:
            self.get_logger().error("error code: %d, error info: %s" % (res_code, res_data))
            raise Exception("error code: %d, error info: %s" % (res_code, res_data))

        for i in xrange(120):
            if not self.table_exists(self._body["datasourceName"]):
                time.sleep(60)
                return
            else:
                self.get_logger().info("waiting for deleting [%s]" % self._body["datasourceName"])
                time.sleep(5)
        self.get_logger().error("failed to delete [%s]" % self._body["datasourceName"])
        raise Exception("failed to delete [%s]" % self._body["datasourceName"])

    def delete_history(self, prefix_table):
        """
        just keep the latest N uindex tables
        """
        regex = re.compile('^%s_\d+' % prefix_table)
        tables = filter(lambda x: re.match(regex, x), self.show_tables())
        tables_his = sorted(tables, reverse=True)[self._latest_n_table:]
        for table in tables_his:
            url = "%s/%s" % (self._uindex_url_delete, table)
            self.get_logger().info(url)
            res_code, res_data = delete(url)
            if res_code != 204 and res_code != 200:
                self.get_logger().error("error code: %d, error info: %s" % (res_code, res_data))
                raise Exception("error code: %d, error info: %s" % (res_code, res_data))
        for i in xrange(120):
            tables_his = filter(self.table_exists, tables_his)
            if not tables_his:
                time.sleep(60)
                return
            else:
                self.get_logger().info("waiting for deleting history [%s]" % str(tables_his))
                time.sleep(5)
        self.get_logger().error("failed to delete history [%s]" % str(tables_his))
        raise Exception("failed to delete history [%s]" % str(tables_his))

    def show_tables(self):
        url = self._uindex_url_show_tables
        self.get_logger().info(url)
        res_code, res_data = get(url)
        if res_code != 200:
            raise Exception("error code: %d, error info: %s" % (res_code, res_data))
        res_data_dict = json.loads(res_data)
        return res_data_dict.keys()

    def table_exists(self, table):
        return table in self.show_tables()

    def runner(self):
        raise NotImplementedError
