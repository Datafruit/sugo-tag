# coding: utf-8

import sys
import numpy as np
import tensorflow as tf
from model.base import BaseAlgorithm
from tools.machine_learning import mse, auc


class LogisticRegressionTFAlgorithm(BaseAlgorithm):

    def __init__(self,
                 w0_default=0.0,
                 w_default=0.0,
                 penalty="L2",
                 C=0.1,
                 step=0.1,
                 max_iter=5000,
                 tol=1e-4):
        """
        :param w0_default: w0 因子的初始值
        :param w_default:  w 因子的初始值
        :param penalty:    正则项, ["L1", "L2"]
        :param C:          正则项强度
        :param step: 
        :param max_iter: 
        :param tol: 
        """
        super(LogisticRegressionTFAlgorithm, self).__init__()
        self._w0_default = w0_default
        self._w_default = w_default
        self._penalty = penalty
        self._C = C
        self._step = step
        self._max_iter = max_iter
        self._tol = tol
        self._w0 = None
        self._w = None

    def fit(self, X, y, *args, **kwargs):
        n_row, n_col = X.shape
        w0 = tf.Variable([self._w0_default], dtype="float32")
        w = tf.Variable(np.full((n_col, 1), self._w_default), dtype="float32")
        if self._penalty == "L2":
            penalty_w0 = self._C * tf.pow(w0, 2)
            penalty_w = self._C * tf.pow(w, 2)
        elif self._penalty == "L1":
            penalty_w0 = tf.Variable([self._C], dtype="float32")
            penalty_w = tf.Variable(np.full((n_col, 1), self._C), dtype="float32")
        else:
            raise Exception("FM must have penalty with one of [L1, L2]")
        xs = tf.constant(np.matrix(X), dtype="float32")
        ys = tf.constant(np.matrix(y), dtype="float32")
        linear = tf.add(w0, tf.matmul(xs, w))
        y_pred = 1. / (1. + tf.exp(-linear))
        # log_loss = -(ys * tf.log(y_pred) + (1. - ys) * tf.log(1. - y_pred))
        square_loss = tf.pow(tf.subtract(ys, y_pred), 2)
        loss = tf.reduce_mean(square_loss) + tf.reduce_mean(penalty_w0) + tf.reduce_mean(penalty_w)
        train_step = tf.train.GradientDescentOptimizer(self._step).minimize(loss)
        init = tf.initialize_all_variables()
        with tf.Session() as sess:
            sess.run(init)
            loss_value = sys.maxint
            mse_value = 1.0
            auc_value = 0.0
            for i in range(self._max_iter):
                ys_eval = ys.eval()
                y_pred_eval = y_pred.eval()
                ## LOSS
                loss_value_new = loss.eval()
                loss_ratio = (loss_value - loss_value_new) / loss_value
                loss_value = loss_value_new
                ## MSE
                mse_value_new = mse(ys_eval, y_pred_eval)
                mse_ratio = (mse_value - mse_value_new) / mse_value
                mse_value = mse_value_new
                ## AUC
                auc_value = auc(ys_eval, y_pred_eval)
                if (i+1) % 100 == 0:
                    self.logger.info("Iterator[%05d]: LOSS[%.6f], MSE[%.6f], AUC[%.6f]" % (i+1, loss_value, mse_value, auc_value))
                if loss_ratio <= self._tol or mse_ratio <= self._tol or (i+1) == self._max_iter:
                    self.logger.info("Iterator[%05d]: LOSS[%.6f], MSE[%.6f], AUC[%.6f]" % (i+1, loss_value, mse_value, auc_value))
                    break
                sess.run(train_step)
            ## save model
            self._w0 = np.matrix(w0.eval())
            self._w = np.matrix(w.eval())
        return self

    def predict(self, X, *args, **kwargs):
        w0 = tf.constant(self._w0.tolist(), dtype="float32")
        w = tf.constant(self._w, dtype="float32")
        xs = tf.constant(np.matrix(X), dtype="float32")
        linear = tf.add(w0, tf.matmul(xs, w))
        y_pred = 1. / (1. + tf.exp(-linear))
        init = tf.initialize_all_variables()
        with tf.Session() as sess:
            sess.run(init)
            sess.run(y_pred)
            y_pred_eval = y_pred.eval()
        return np.matrix(y_pred_eval)
