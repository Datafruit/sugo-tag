# coding: utf-8

import sys
import numpy as np
import scipy as sp
from scipy.sparse import csr_matrix
from model.base import BaseAlgorithm
from tools.machine_learning import mse, auc, logloss


class FactorizationMachineAlgorithm(BaseAlgorithm):

    def __init__(self,
                 w0_default=0.0,
                 w_default=0.0,
                 v_default=1.0,
                 v_length=3,
                 penalty="L2",
                 C=0.05,
                 step=0.1,
                 max_iter=500,
                 tol=1e-4):
        """
        :param w0_default: w0 因子的初始值
        :param w_default:  w 因子的初始值
        :param v_default:  v 因子的初始值, 没用到, 采用的是 [0, 1) 的随机数
        :param v_length:   v 因子向量的长度
        :param penalty:    正则项, ["L1", "L2"]
        :param C:          正则项强度
        :param step: 
        :param max_iter: 
        :param tol: 
        """
        super(FactorizationMachineAlgorithm, self).__init__()
        self._w0_default = w0_default
        self._w_default = w_default
        self._v_default = v_default
        self._v_length = v_length
        self._penalty = penalty
        self._C = C
        self._step = step
        self._max_iter = max_iter
        self._tol = tol
        self._w0 = None
        self._w = None
        self._v = None

    def fit(self, X, y, *args, **kwargs):
        n_row, n_col = X.shape
        ## convert to scipy.sparse_matrix
        X = csr_matrix(X, dtype=np.float64)
        y = csr_matrix(y, dtype=np.float64)
        x_sts = np.matrix(X.transpose().sum(axis=1))
        x_ratio = csr_matrix(np.divide(1.0, x_sts, out=np.zeros_like(x_sts), where=x_sts!=0))
        ## init factors
        w0 = self._w0_default
        w = csr_matrix(np.full((n_col, 1), self._w_default))
        v = csr_matrix(np.random.randn(n_col, self._v_length))
        ## train model
        mse_value = 1.0
        logloss_value = sys.maxint
        auc_value = 0.0
        for i in xrange(self._max_iter):
            ## delta
            delta = self.sig_mod(X, w0, w, v) - y
            ## descent
            descent_w0 = np.sum(delta)
            descent_w = X.transpose().dot(delta)
            descent_v_list = list()
            for v1 in v.T:
                derivative_v1 = X.multiply(X.dot(v1.transpose())) - X.power(2).multiply(v1)
                descent_v1 = derivative_v1.transpose().dot(delta)
                descent_v_list.append(descent_v1.transpose().toarray()[0])
            descent_v = csr_matrix(np.matrix(descent_v_list).transpose())
            ## penalty
            if self._penalty == "L2":
                penalty_w0 = self._C * w0
                penalty_w = self._C * w
                penalty_v = self._C * v
            elif self._penalty == "L1":
                penalty_w0 = self._C
                penalty_w = csr_matrix(np.full((n_col, 1), self._C))
                penalty_v = csr_matrix(np.full((n_col, self._v_length), self._C))
            else:
                raise Exception("FM must have penalty with one of [L1, L2]")
            ## gradient descent
            w0 = w0 - self._step * (descent_w0 / float(n_row) + penalty_w0)
            w = w - self._step * (descent_w.multiply(x_ratio) + penalty_w)
            v = v - self._step * (descent_v.multiply(x_ratio) + penalty_v)
            ## evaluate
            if (i + 1) % 10 == 0 or (i + 1) == self._max_iter:
                y_true = y.toarray()
                y_pred = self.sig_mod(X, w0, w, v).toarray()
                ## MSE
                mse_value_new = mse(y_true, y_pred)
                mse_ratio = (mse_value - mse_value_new) / mse_value
                mse_value = mse_value_new
                ## LogLoss
                logloss_value_new = logloss(y_true, y_pred)
                logloss_ratio = (logloss_value - logloss_value_new) / logloss_value
                logloss_value = logloss_value_new
                ## AUC
                auc_value = auc(y_true, y_pred)
                # self.logger.info("Iterator[%05d]: MSE[%.6f], LogLoss[%.6f], AUC[%.6f]" % (i+1, mse_value, logloss_value, auc_value))
                if mse_ratio <= self._tol or logloss_ratio <= self._tol or (i+1) == self._max_iter:
                    self.logger.info("Iterator[%05d]: MSE[%.6f], LogLoss[%.6f], AUC[%.6f]" % (i+1, mse_value, logloss_value, auc_value))
                    break
        ## save model
        self._w0 = w0
        self._w = w
        self._v = v
        return self

    def predict(self, X, *args, **kwargs):
        X = csr_matrix(X)
        return self.sig_mod(X, self._w0, self._w, self._v).todense()

    def sig_mod(self, X, w0, w, v):
        h = csr_matrix(np.full((X.shape[0], 1), w0)) + \
            X.dot(w) + \
            0.5 * sp.sum(X.dot(v).power(2) - X.power(2).dot(v.power(2)), axis=1)
        sm = 1.0 / (1.0 + np.exp(-1.0 * h))
        return csr_matrix(np.nan_to_num(sm))
