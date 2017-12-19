# coding: utf-8

import json
import itertools
from task.base import BaseTaskWithParams


class TagSortingTask(BaseTaskWithParams):

    _data_total = None  ## object of data.base.BaseData
    _user_group_cls = None  ## class of data.base.BaseData
    _model = None  ## class of model.key_feature.KeyFeatureModel
    _parallel_task = 2

    def __init__(self, param):
        super(TagSortingTask, self).__init__(param)
        self._group_id = list(self._param.value["group_id"])
        self._repo_data_file = self._param.value["repo_data_file"]
        assert len(self._group_id) > 0

    def parallel_runner(self, *args):
        ret = {"result": []}
        for model in args:
            for lineage in model._lineages_fmt:
                decision = lineage["decisions"][0]
                if not decision["floor"]:
                    continue
                tag = decision["name"]
                prop = lineage["prop"]
                gini = lineage["gini"]
                ratio_user_group = prop[0]
                ratio_compare = prop[1]
                ratio = sum(lineage["value"]) / sum(lineage["total"])
                if isinstance(tag, unicode):
                    tag = tag.encode("utf-8")
                dimension, tag_name = tag.split('`')
                ret["result"].append({
                    "dimension": dimension,
                    "tagName": tag_name,
                    "f": gini,
                    "ratio": ratio,
                    "ratioUsergroup": ratio_user_group,
                    "ratioCompare": ratio_compare
                })
        with open(self._repo_data_file, 'w') as fd:
            print >> fd, json.dumps(ret)

    @classmethod
    def parallel_function(cls, model, X, y):
        model.fit(X, y)
        return model

    def parallel_args_iter(self):
        data_train_list = []
        data_other = self._data_total
        idx = -1
        for idx, gid in enumerate(self._group_id):
            user_group = self._user_group_cls(group_name=gid)
            data_group = self._data_total.intersection(user_group, user_group._col_key)
            data_group.set_label(str(idx))
            if data_group.get_shape()[0] == 0:
                self.get_logger().error("number of group[%s] is 0" % gid)
                raise Exception("number of group[%s] is 0" % gid)
            data_train_list.append(data_group)
            data_other = data_other.difference(user_group, user_group._col_key)
        data_other.set_label(str(idx + 1))
        if data_other.get_shape()[0] == 0:
            self.get_logger().error("number of group[other] is 0")
            raise Exception("number of group[other] is 0")
        data_train_list.append(data_other)

        data_train = data_train_list[0].get_data()
        for data in data_train_list[1:]:
            data_train = data_train.append(data.get_data())
        X = data_train[self._data_total._col_feature]
        X.fillna(0, inplace=True)
        col_label = self._data_total._col_label if \
            isinstance(self._data_total._col_label, list) else \
            [self._data_total._col_label]
        y = data_train[col_label]
        for feature in list(itertools.combinations(X.columns.tolist(), 1)):
            yield [self._model(), X[[f for f in feature]], y]

