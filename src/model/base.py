# coding: utf-8

import copy
import logging
import numpy as np
from sklearn.model_selection import KFold
from sklearn.metrics import roc_curve, f1_score
from tools.machine_learning import mse
from tools.machine_learning import auc


def algorithm_proxy(alg_cls, alg_kwargs, X_train, y_train, train_kwargs):
    alg = alg_cls(**alg_kwargs)
    alg = alg.fit(X_train, y_train, **train_kwargs)
    return alg


def kfold_cross_validation(alg, X, y, k, valid_threshold=0.9):
    """
    :param alg: 算法模型实例 
    :param X:   
    :param y:   
    :param k: kfold算法的数目
    :param valid_threshold: 验证通过的阈值(训练集和测试集预测结果的余弦相似度)
    :return: mse, auc, threshold
    """
    mse_kfold = {"train": [], "test": []}
    auc_kfold = {"train": [], "test": []}
    threshold_kfold = []
    kf = KFold(n_splits=k, shuffle=True)
    for idx, (train_idx, test_idx) in enumerate(kf.split(y)):
        X_train = X.iloc[train_idx,:]
        y_train = y.iloc[train_idx,:]
        X_test = X.iloc[test_idx,:]
        y_test = y.iloc[test_idx,:]
        alg_cp = copy.deepcopy(alg)
        alg_cp = alg_cp.fit(X_train, y_train)
        y_train_pred = alg_cp.predict(X_train)
        y_test_pred = alg_cp.predict(X_test)
        mse_train_value = mse(y_train, y_train_pred)
        auc_train_value = auc(y_train, y_train_pred)
        mse_test_value = mse(y_test, y_test_pred)
        auc_test_value = auc(y_test, y_test_pred)
        fpr, tpr, threshold = roc_curve(y_test, y_test_pred)
        f1score = []
        for thr in threshold:
            y_test_pred_class = np.apply_along_axis(func1d=lambda x: 1 if x >= thr else 0, axis=1, arr=y_test_pred)
            f1score.append(f1_score(y_test, y_test_pred_class))
        threshold_optimal = threshold[np.argmax(f1score)]
        mse_kfold["train"].append(mse_train_value)
        mse_kfold["test"].append(mse_test_value)
        auc_kfold["train"].append(auc_train_value)
        auc_kfold["test"].append(auc_test_value)
        threshold_kfold.append(threshold_optimal)
        alg.logger.info(
            "cross validation fold[%d]: mse_train[%.6f], mse_test[%.6f], auc_train[%.6f], auc_test[%.6f], threshold[%.6f]" %
            (idx, mse_train_value, mse_test_value, auc_train_value, auc_test_value, threshold_optimal)
        )
    cos_sim_mse = \
        np.dot(mse_kfold["train"], mse_kfold["test"]) / \
        (np.linalg.norm(mse_kfold["train"]) * np.linalg.norm(mse_kfold["test"]))
    cos_sim_auc = \
        np.dot(auc_kfold["train"], auc_kfold["test"]) / \
        (np.linalg.norm(auc_kfold["train"]) * np.linalg.norm(auc_kfold["test"]))
    alg.logger.info("cross validation: cosine similarity of mse: %.6f" % cos_sim_mse)
    alg.logger.info("cross validation: cosine similarity of auc: %.6f" % cos_sim_auc)
    if cos_sim_mse < valid_threshold or cos_sim_auc < valid_threshold:
        alg.logger.error("cross validation: algorithm is overfit")
        raise Exception("%s cross validation: algorithm is overfit" % alg.__class__.__name__)
    return np.mean(mse_kfold["test"]), np.mean(auc_kfold["test"]), np.mean(threshold_kfold)


class BaseAlgorithm(object):

    _logger_setter = False

    @property
    def logger(self):
        if not self.__class__._logger_setter:
            logger = logging.getLogger(self.__class__.__name__)
            logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s %(name)-15s %(levelname)-8s %(message)s")
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.DEBUG)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            self.__class__._logger_setter = True
        return logging.getLogger(self.__class__.__name__)

    def fit(self, X, y, *args, **kwargs):
        return self

    def predict(self, X, *args, **kwargs):
        pass


class BaseModel(object):

    _logger_setter = False

    def __init__(self, *args, **kwargs):
        self._alg = self.get_algorithm()
        assert "fit" in dir(self._alg)
        assert "predict" in dir(self._alg)

    @property
    def logger(self):
        if not self.__class__._logger_setter:
            logger = logging.getLogger(self.__class__.__name__)
            logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s %(name)-15s %(levelname)-8s %(message)s")
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.DEBUG)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
            self.__class__._logger_setter = True
        return logging.getLogger(self.__class__.__name__)

    def get_algorithm(self):
        """
        :return: obj with method of fit(X, y) and predict(X)
        """
        raise NotImplementedError

    def fit(self, X, y, *args, **kwargs):
        self._alg = self._alg.fit(X, y, *args, **kwargs)
        return self

    def predict(self, X, *args, **kwargs):
        return self._alg.predict(X, *args, **kwargs)

    def estimate(self, X, y):
        y_actual = y
        y_predict = self.predict(X)
        ## TODO: return the precision of y_actual compare with y_predict
        return
