# coding: utf-8

import json

from conf.env import DRUID_URL_PRETTY
from tools.http_method import post
from task.base import BaseTask


class DruidQueryGroupTask(BaseTask):

    _druid_url_pretty = DRUID_URL_PRETTY

    def __init__(self):
        super(DruidQueryGroupTask, self).__init__()
        self._body = {
            "queryType": "lucene_select",
            "dataSource": None,
            "intervals": "1000/3000",
            "descending": True,
            "dimensions": [],
            "emptyToAllDims": True,
            "storedFields": True,
            "filter": None,
            "granularity": "all",
            "pagingSpec": {"pagingIdentifiers":{}, "threshold": 50},
            "context": {
                "timeout": 60000,
                "groupByStrategy": "v2",
                "signal": {
                    "aborted": False,
                    "onabort": None
                }
            }
        }
        self._filter_in = {"type": "lookup", "dimension": None, "lookup": None}
        self._filter_not_in = {"type": "not", "field": {"type": "lookup", "dimension": None, "lookup": None}}

    def set_table_name(self, name):
        self._body["dataSource"] = name

    def set_group_name(self, name):
        group = "usergroup_%s" % name
        self._filter_in["lookup"] = group
        self._filter_not_in["field"]["lookup"] = group

    def add_key(self, key):
        self._filter_in["dimension"] = key
        self._filter_not_in["field"]["dimension"] = key

    def get_group(self):
        url = self._druid_url_pretty
        content_type = "application/json; charset=utf-8"
        self._body["filter"] = self._filter_in
        data_json = json.dumps(self._body)
        self.get_logger().info("%s\n%s\n%s" % (url, content_type, data_json))
        res_code, res_data = post(url, data_json, content_type)
        if res_code != 200:
            self.get_logger().error("error code: %d" % res_code)
            raise Exception("error code: %d" % res_code)
        return res_data

    def get_other(self):
        url = self._druid_url_pretty
        content_type = "application/json; charset=utf-8"
        self._body["filter"] = self._filter_not_in
        data_json = json.dumps(self._body)
        self.get_logger().info("%s\n%s\n%s" % (url, content_type, data_json))
        res_code, res_data = post(url, data_json, content_type)
        if res_code != 200:
            self.get_logger().error("error code: %d" % res_code)
            raise Exception("error code: %d" % res_code)
        return res_data

    def parse_data(self, data):
        for page in json.loads(data):
            for offset in page["result"]["events"]:
                event = offset["event"]
                yield event

    def runner(self):
        raise NotImplementedError