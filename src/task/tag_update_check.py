# coding: utf-8

import time
import json
from conf.env import DRUID_URL_PRETTY
from task.base import BaseTask
from sql.hive_tag_collector import HiveTagCollectorSql
from tools.command import Command
from tools.http_method import get, post, delete


class TagUpdateCheckTask(BaseTask):

    _hive_db = HiveTagCollectorSql._db_dst
    _hive_table_dst = HiveTagCollectorSql._table_dst
    _hive_table_dict = HiveTagCollectorSql._table_dict
    _hive_task_log_file = None
    _druid_url_pretty = DRUID_URL_PRETTY
    _uindex_table_dst = None
    _uindex_table_dict = None

    def hive_count(self, table):
        args = """
tail -1000 {hive_task_log_file} | grep -E "{hive_db}[[:space:]]+{hive_table}[[:space:]]+" | tail -1 | awk '{{print $3}}'
""".format(
            hive_task_log_file=self._hive_task_log_file,
            hive_db = self._hive_db,
            hive_table=table
        )
        self.get_logger().info(args)
        return int(Command.run_output(args, self.get_log_file()))

    def uindex_count(self, table):
        url = self._druid_url_pretty
        content_type = "application/json; charset=utf-8"
        body = {
            "queryType": "lucene_timeseries",
            "dataSource": "%s" % table,
            "intervals": "1000/3000",
            "granularity": "all",
            "context": {
                "timeout": 60000,
                "groupByStrategy": "v2",
                "useCache": False
            },
            "aggregations": [{"name": "__VALUE__", "type": "lucene_count"}]
        }
        data_json = json.dumps(body)
        self.get_logger().info("%s\n%s\n%s" % (url, content_type, data_json))
        res_code, res_data = post(url, data_json, content_type)
        if res_code != 200:
            self.get_logger().error("error code: %d, error info: %s" % (res_code, res_data))
            raise Exception("error code: %d, error info: %s" % (res_code, res_data))
        res_data_decode = json.loads(res_data)
        return res_data_decode[0]["result"]["__VALUE__"] if len(res_data_decode) > 0 else -1

    def runner(self):
        for i in xrange(24):
            self.get_logger().info("check if tag update seccess")
            cnt_hive_dst = self.hive_count(self._hive_table_dst)
            cnt_uindex_dst = self.uindex_count(self._uindex_table_dst)
            cnt_hive_dict = self.hive_count(self._hive_table_dict)
            cnt_uindex_dict = self.uindex_count(self._uindex_table_dict)
            self.get_logger().info("cnt_hive_dst: %d, cnt_uindex_dst: %d. cnt_hive_dict: %d, cnt_uindex_dict: %d" % (cnt_hive_dst, cnt_uindex_dst, cnt_hive_dict, cnt_uindex_dict))
            if cnt_hive_dst == cnt_uindex_dst and cnt_hive_dict == cnt_uindex_dict:
                self.get_logger().info("tag update seccess")
                return
            time.sleep(5)
        self.get_logger().error("tag update failed")
        raise Exception("tag update failed")
