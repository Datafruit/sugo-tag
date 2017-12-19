# coding: utf-8

import json
import urllib2
from conf.env import WEB_URL_UPDATE_DIMENSION
from task.base import BaseTask


class WebUpdateTask(BaseTask):

    _web_url_update_dimension = WEB_URL_UPDATE_DIMENSION
    _uindex_datasource_prefix = None
    _uindex_datasource_profile = None
    _uindex_datasource_profile_tag = None

    def update_dimensions(self):
        assert self._uindex_datasource_profile is not None
        assert self._uindex_datasource_profile_tag is not None
        url = self._web_url_update_dimension.format(
            datasource_prefix=self._uindex_datasource_prefix,
            datasource_profile=self._uindex_datasource_profile,
            datasource_profile_tag=self._uindex_datasource_profile_tag
        )
        self.get_logger().info(url)
        req = urllib2.Request(url=url)
        req.get_method = lambda : 'GET'
        res = urllib2.urlopen(req)
        res_data = res.read()
        res_json = json.loads(res_data)
        if (not res_json.has_key("success")) or (not res_json["success"]):
            self.get_logger().error("failed to update dimensions, error info: %s" % res_data)
            raise Exception("failed to update dimensions, error info: %s" % res_data)
        else:
            self.get_logger().info("success to update dimensions, %s" % res_data)

    def update_flag(self):
        pass

    def runner(self):
        raise NotImplementedError
