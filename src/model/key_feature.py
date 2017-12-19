# coding: utf-8

import numpy as np
from sklearn import tree
from sklearn.tree import _tree
from model.base import BaseModel


class KeyFeatureModel(BaseModel):

    _criterion = "gini"
    _max_depth = 7
    _min_samples_split = 0.001
    _min_samples_leaf = 0.001
    _min_impurity_split = 0.001

    def __init__(self):
        """
        self._lineages_fmt:
            {
                "value": [], "total": [], "prop": [], "gini": , 
                "decisions": [{"name": , "floor": , "upper": }, ]
            }
        PS: 
            value: 每个类别根据 decisions 筛选后的数目
            total: 总的样本数
            prop: 每个类别根据 decisions 筛选后占 total 的比例
            gini: 基尼指数
            decisions: 决策组合
            decisions.name: 特征名
            decisions.floor: 范围下限, None 为无穷小
            decisions.upper: 范围上限, None 为无穷大
        """
        super(KeyFeatureModel, self).__init__()
        self._lineages = []
        self._lineages_fmt = []
        self._classes = []

    def get_algorithm(self):
        return tree.DecisionTreeClassifier(
            criterion=self._criterion,
            max_depth=self._max_depth,
            min_samples_split=self._min_samples_split,
            min_samples_leaf=self._min_samples_leaf,
            min_impurity_split=self._min_impurity_split
        )

    def fit(self, X, y, *args, **kwargs):
        super(KeyFeatureModel, self).fit(X, y)
        self.lineage_extract(self._alg, X.columns)
        lineages_dst = self.lineage_distinct(self._lineages)
        lineages_cmb = [self.lineage_combine(lineage) for lineage in lineages_dst]
        for lineage in lineages_cmb:
            lineage["gini"] = self.lineage_gini(lineage)
            lineage["prop"] = [float(v) / float(lineage["total"][idx]) for idx, v in enumerate(lineage["value"])]
        self._lineages_fmt = sorted(lineages_cmb, key=lambda x: x["gini"], reverse=False)

    ## 将 sklearn 的决策扁平化
    def tree_lineage(self, tree, feature_names):
        lineages = []
        lineage_one = [
            {
                "name":None,
                "threshold":None,
                "value":[],
                "direction":None,
                "isleaf":None,
                "impurity":None
            } for i in range(tree.max_depth + 1)
        ]

        def depth_first_search(node, depth):
            feature = tree.feature[node]
            if feature == _tree.TREE_UNDEFINED:
                lineage_one[depth-1]["name"] = None
                lineage_one[depth-1]["threshold"] = None
                lineage_one[depth-1]["value"] = []
                for v in tree.value[node][0]:
                    lineage_one[depth-1]["value"].append(v)
                lineage_one[depth-1]["isleaf"] = True
                lineage_one[depth-1]["impurity"] = tree.impurity[node]
                lineage_one[depth-1]["direction"] = None
                lineages.append([info.copy() for info in lineage_one[:depth]])
            else:
                lineage_one[depth-1]["name"] = feature_names[feature]
                lineage_one[depth-1]["threshold"] = tree.threshold[node]
                lineage_one[depth-1]["value"] = []
                for v in tree.value[node][0]:
                    lineage_one[depth-1]["value"].append(v)
                lineage_one[depth-1]["isleaf"] = False
                lineage_one[depth-1]["impurity"] = tree.impurity[node]
                node_left = tree.children_left[node]
                node_right = tree.children_right[node]
                lineage_one[depth-1]["direction"] = "left"
                depth_first_search(node_left, depth + 1)
                lineage_one[depth-1]["direction"] = "right"
                depth_first_search(node_right, depth + 1)

        depth_first_search(0, 1)
        for lineage in lineages:
            lineage_new = list()
            total = list()
            for v in lineage[0]["value"]:
                total.append(v)
            for idx, v in enumerate(lineage[:-1]):
                floor = v["threshold"] if v["direction"].lower() == "right" else None
                upper = v["threshold"] if v["direction"].lower() == "left" else None
                decision = {
                    "name":v["name"],
                    "floor":floor,
                    "upper":upper,
                    "value":lineage[idx+1]["value"][:],
                    "impurity":lineage[idx+1]["impurity"],
                    "total":total
                }
                lineage_new.append(decision)
            yield lineage_new

    ## 从模型中抽出树结构
    def lineage_extract(self, tree_alg, column_names):
        if not self._classes:
            self._classes = [v for v in tree_alg.classes_]
        for lineage in self.tree_lineage(tree_alg.tree_, column_names):
            self._lineages.append(lineage)
            ## 暂时不把多维决策打散成低纬决策
            # for lineage_bk in self.lineage_breakup(lineage):
            #     self._lineages.append(lineage_bk)

    ## 将一个决策打散成多个决策
    def lineage_breakup(self, lineage):
        for i in range(1, len(lineage)+1):
            lineage_new = list()
            for decision in lineage[:i]:
                lineage_new.append(decision)
            yield lineage_new

    ## 决策去重
    def lineage_distinct(self, lineages):
        decision_dict = dict()
        def add_lineage(lineage):
            for decision in lineage:
                name = decision["name"]
                floor = decision["floor"]
                upper = decision["upper"]
                if not decision_dict.has_key(name):
                    decision_dict[name] = {"floor":set(),"upper":set()}
                decision_dict[name]["floor"].add(floor)
                decision_dict[name]["upper"].add(upper)
        def has_lineage(lineage):
            for decision in lineage:
                name = decision["name"]
                floor = decision["floor"]
                upper = decision["upper"]
                if decision_dict.has_key(name):
                    if floor not in decision_dict[name]["floor"] or \
                                    upper not in decision_dict[name]["upper"]:
                        return False
                else:
                    return False
            return True
        lineages_new = list()
        for lineage in lineages:
            if not has_lineage(lineage):
                lineages_new.append(lineage)
                add_lineage(lineage)
        return lineages_new

    ## 决策合并
    def lineage_combine(self, lineage):
        lineage_cmb = list()
        for decision in lineage:
            combine = False
            for decision_new in lineage_cmb:
                if decision["name"] == decision_new["name"]:
                    if decision["floor"] and decision_new["upper"]:
                        decision_new["floor"] = max(decision_new["floor"], decision["floor"])
                    elif decision["floor"]:
                        decision_new["floor"] = decision["floor"]
                    if decision["upper"] and decision_new["upper"]:
                        decision_new["upper"] = min(decision_new["upper"], decision["upper"])
                    elif decision["upper"]:
                        decision_new["upper"] = decision["upper"]
                    combine = True
            if not combine:
                lineage_cmb.append(decision)
        lineage_new = {
            "decisions":[],
            "value":lineage[-1]["value"],
            "total":lineage[-1]["total"]
        }
        for decision in lineage_cmb:
            lineage_new["decisions"].append(
                {
                    "name":decision["name"],
                    "floor":decision["floor"],
                    "upper":decision["upper"]
                }
            )
        return lineage_new

    ## numpy.matrix:
    ##            cate1 cate2 cate3 ...
    ## decision1  n11   n12   n13   ...
    ## decision2  n21   n22   n23   ...
    ## decision3  n31   n32   n33   ...
    ##    ...     ...   ...   ...   ...
    def gini(self, X):
        # 一些统计工作
        n_row, n_col = X.shape
        Cn = X.sum(axis=0)
        C = Cn.sum()

        # 纠正样本偏差
        CW = (Cn / C).tolist()[0]
        X = np.apply_along_axis(lambda x: x/CW, axis=1, arr=X)

        # 纠正后的样本再做一些统计工作
        Dn = X.sum(axis=1)

        # 计算基尼系数
        X = np.apply_along_axis(lambda x: x/Dn, axis=0, arr=X)
        X[np.isnan(X)] = 1.0 / float(n_col)
        X = np.power(X, 2)
        gn = 1.0 - X.sum(axis=1)
        gn = gn * (1.0 / float(n_row))
        return 1.0 - gn.sum() / (1.0 - 1.0 / float(n_col))

    def lineage_gini(self, lineage):
        value = lineage["value"]
        total = lineage["total"]
        X = np.matrix(
            data = [
                [v for v in value],
                [total[idx] - v for idx, v in enumerate(value)]
            ],
            dtype=np.float64
        )
        return self.gini(X)