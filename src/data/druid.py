# coding: utf-8

import json
import pandas as pd
from conf.env import DRUID_URL_PRETTY
from tools.http_method import post
from data.base import BaseData


class BaseDataDruid(BaseData):

    _druid_url_pretty = DRUID_URL_PRETTY
    _data_source = None

    def __init__(self, data=None, group_name=None):
        assert self._data_source is not None
        self._group_name = group_name
        self._body = {
            "queryType": "lucene_scan",
            "dataSource": self._data_source,
            "resultFormat": "compactedList",
            "batchSize": 100,
            "limit": 0,
            "columns": self._col_all,
            "filter": {"type": "lookup", "dimension": self._col_key[0], "lookup": "usergroup_%s" % self._group_name} if self._group_name is not None else None,
            "intervals": "1000/3000"
        }
        self._body_count = {
            "queryType": "lucene_timeseries",
            "dataSource": self._data_source,
            "intervals": "1000/3000",
            "filter": {"type": "lookup", "dimension": self._col_key[0], "lookup": "usergroup_%s" % self._group_name} if self._group_name is not None else None,
            "granularity": "all",
            "context": {
                "timeout": 180000,
                "groupByStrategy": "v2",
                "useCache": False
            },
            "aggregations": [
                {
                    "name": "__VALUE__",
                    "type": "lucene_count"
                }
            ]
        }
        super(BaseDataDruid, self).__init__(data)

    def read_data(self):
        return self.read_druid()

    def read_druid(self):
        url = self._druid_url_pretty
        content_type = "application/json; charset=utf-8"
        body_count_json = json.dumps(self._body_count)
        res_code, res_data = post(url, body_count_json, content_type)
        if res_code != 200:
            raise Exception("error code: %d" % res_code)
        limit = json.loads(res_data)[0]["result"]["__VALUE__"]

        self._body["limit"] = limit
        body_json = json.dumps(self._body)
        res_code, res_data = post(url, body_json, content_type)
        if res_code != 200:
            raise Exception("error code: %d" % res_code)
        data = []
        for segment in json.loads(res_data):
            columns_name = segment["columns"]
            for event in segment["events"]:
                data.append(dict(zip(columns_name, event)))
        return pd.DataFrame(data)

