# coding: utf-8

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.externals.joblib import Parallel, delayed
from model.fm import FactorizationMachineAlgorithm
from model.base import BaseModel
from model.base import algorithm_proxy


class LittleProbabilityModel(BaseModel):

    def __init__(self, n_jobs=4):
        super(LittleProbabilityModel, self).__init__()
        self._n_jobs = n_jobs
        self._centers = None
        self._algs = []

    def get_algorithm(self):
        return FactorizationMachineAlgorithm

    def fit(self, X, y, *args, **kwargs):
        self.logger.info("prepare")
        X_pos = X.loc[y[y["__label__"] == 1].index.tolist(),:].reset_index(drop=True)
        X_neg = X.loc[y[y["__label__"] == 0].index.tolist(),:].reset_index(drop=True)
        y_pos = y.loc[y[y["__label__"] == 1].index.tolist(),:].reset_index(drop=True)
        y_neg = y.loc[y[y["__label__"] == 0].index.tolist(),:].reset_index(drop=True)
        neg_split = y_neg.shape[0] / y_pos.shape[0] / 10
        neg_split = max(neg_split, 2)
        ## 负样本样本分块, 并记录每个分块的中心
        self.logger.info("cluster")
        km = MiniBatchKMeans(
            batch_size=X_neg.shape[0] / 10000 if X_neg.shape[0] > 10000 else 2,
            n_clusters = neg_split
        )
        km = km.fit(X_neg)
        regions = pd.DataFrame(km.labels_.transpose(), columns=["__region__"])
        self._centers = np.matrix(km.cluster_centers_)
        ## 每个分块都和全部正样本组合成一个训练集, 并独立训练一个模型
        self.logger.info("train")
        def parallel_args():
            for i in xrange(neg_split):
                X_neg_train_index = regions[regions["__region__"] == i].index.tolist()
                X_neg_train = X_neg.loc[X_neg_train_index,:]
                y_neg_train = y_neg.loc[X_neg_train_index,:]
                X_train = X_neg_train.append(X_pos, ignore_index=True)
                y_train = y_neg_train.append(y_pos, ignore_index=True)
                yield X_train, y_train
        self._algs = Parallel(n_jobs=self._n_jobs, verbose=1, backend="multiprocessing")(
            [
                delayed(algorithm_proxy)(self._alg, {}, X_train, y_train, {})
                for X_train, y_train in parallel_args()
            ]
        )
        return self

    def predict(self, X, *args, **kwargs):
        ## 保证 X 的序列在分块预测后不被打乱
        X_idx = {v: i for i, v in enumerate(X.index.tolist())}
        M = np.matrix(X)
        C = self._centers
        ## 分块
        regions = np.argmin(
            np.add(
                np.power(M, 2).sum(axis=1),
                np.power(C, 2).sum(axis=1).transpose()
            ) - 2.0 * np.dot(M, C.transpose()),
            axis=1
        )
        ## 预测
        def score(x):
            region = x["__region__"].iloc[0]
            alg = self._algs[region]
            idx = [X_idx[v] for v in x.index.tolist()]
            col = filter(lambda c: c != "__region__", x.columns.tolist())
            s = pd.DataFrame(alg.predict(x[col]), index=idx)
            return s
        X["__region__"] = regions
        return np.matrix(
            X.groupby("__region__")
            .apply(score)
            .reset_index(level=0)
            .drop(["__region__"], axis=1)
            .sort_index()
        )
