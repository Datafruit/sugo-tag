# coding: utf-8

import json
from task.base import BaseTaskWithParams


class TagRecommendTask(BaseTaskWithParams):

    _data_total = None  ## object of data.base.BaseData
    _model = None  ## class of model.recommend.RecommendModel

    def __init__(self, param):
        super(TagRecommendTask, self).__init__(param)
        self._repo_data_file = self._param.value["repo_data_file"]
        self._tag = self._param.value["tag"]
        self._tag_from = self._param.value["tag_from"]
        self._tag_to = self._param.value["tag_to"]
        self._topn = self._param.value["topn"]

    def runner(self):
        tags_name = filter(lambda x: x.startswith(self._tag), self._data_total._col_feature)
        data_total = self._data_total.get_data()
        data_total.fillna(0, inplace=True)

        data_from = data_to = None
        if self._tag_from:
            tag_name = self._tag + '`' + self._tag_from
            data_from = data_total[data_total[tag_name] == 1]
        else:
            data_from = data_total
            for tag_name in tags_name:
                data_from = data_from[data_from[tag_name] == 0]
        data_from["__label__"] = 0

        if self._tag_to:
            tag_name = self._tag + '`' + self._tag_to
            data_to = data_total[data_total[tag_name] == 1]
        else:
            data_to = data_total
            for tag_name in tags_name:
                data_to = data_to[data_to[tag_name] == 0]
        data_to["__label__"] = 1

        data_train = data_from.append(data_to, ignore_index=True)
        train_columns = filter(lambda x: x not in tags_name, self._data_total._col_feature)
        label_columns = ["__label__"]
        X = data_train[train_columns]
        y = data_train[label_columns]
        model = self._model()
        model.fit(X, y)

        X_predict = data_from[train_columns]
        y_predict = model.predict(X_predict)
        key_columns = self._data_total._col_key
        result = data_from[key_columns]
        result["score"] = y_predict
        result.sort_values(by=["score"], ascending=False, inplace=True)

        ret = {"result": []}
        key_name = '_'.join(key_columns)
        for idx, row in result.head(self._topn).iterrows():
            key = '_'.join([row[key_col] for key_col in key_columns])
            score = row["score"]
            ret["result"].append({key_name: key, "score": score})
        with open(self._repo_data_file, 'w') as fd:
            print >> fd, json.dumps(ret)

