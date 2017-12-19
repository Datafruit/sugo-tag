# coding: utf-8

from conf.env import TASK_DATA_HOME
from tools.machine_learning import one_hot_coding
from tools.machine_learning import discretization_coding
from tools.machine_learning import mse
from tools.machine_learning import auc
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from model.base import kfold_cross_validation
from model.lr import LogisticRegressionAlgorithm
from model.fm import FactorizationMachineAlgorithm
from model.little_probability import LittleProbabilityModel


def run():
    data_home = TASK_DATA_HOME + "/algorithm_test/kaggle/creditcardfraud"
    df = pd.read_csv(data_home + "/creditcard.sub.csv")
    df.reset_index(inplace=True)
    id = "index"
    features_continuous = filter(lambda x: x not in ("Class", id), df.columns.tolist())
    features_discrete = []
    df["__label__"] = df["Class"].apply(lambda x: 1 if int(x) == 1 else 0)
    label = "__label__"
    df_coding = df[[id, label]]
    for feature in features_continuous:
        df_coding_f = discretization_coding(df, id, feature)
        df_coding = df_coding.merge(df_coding_f, how="inner", on=id)
    for feature in features_discrete:
        df_coding_f = one_hot_coding(df, id, feature)
        df_coding = df_coding.merge(df_coding_f, how="inner", on=id)
    df_coding = df_coding.sample(frac=1).reset_index(drop=True)

    x_columns = filter(lambda x: x not in (id, label), df_coding.columns)
    y_columns = [label]
    X = df_coding[x_columns]
    y = df_coding[y_columns]

    index_pos = y[y["__label__"] == 1].index.tolist()
    index_neg = y[y["__label__"] == 0].index.tolist()
    index_pos_train, index_pos_test = train_test_split(index_pos, test_size=0.2)
    index_neg_train, index_neg_test = train_test_split(index_neg, test_size=0.2)
    X_train = X.loc[index_pos_train + index_neg_train,:]
    X_test = X.loc[index_pos_test + index_neg_test,:]
    y_train = y.loc[index_pos_train + index_neg_train,:]
    y_test = y.loc[index_pos_test + index_neg_test,:]

    # print "############ lr ############"
    # algo_lr = LogisticRegressionAlgorithm()
    # algo_lr = algo_lr.fit(X_train, y_train)
    # algo_lr.predict(X_test)
    # y_predict_lr = pd.DataFrame(algo_lr.predict(X_test), columns=["__label__"])
    # print "lr  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_lr), auc(y_test, y_predict_lr))
    #
    # print "############ gbdt ############"
    # algo_gbdt = GradientBoostingRegressor()
    # algo_gbdt = algo_gbdt.fit(X_train, y_train)
    # y_predict_gbdt = pd.DataFrame(algo_gbdt.predict(X_test), columns=["__label__"])
    # print "gbdt: mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_gbdt), auc(y_test, y_predict_gbdt))
    #
    # print "############ fm ############"
    # algo_fm = FactorizationMachineAlgorithm()
    # algo_fm = algo_fm.fit(X_train, y_train)
    # y_predict_fm = pd.DataFrame(algo_fm.predict(X_test), columns=["__label__"])
    # print "fm  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_fm), auc(y_test, y_predict_fm))

    print "############ lp ############"
    algo_lp = LittleProbabilityModel(n_jobs=2)
    kfold_cross_validation(algo_lp, X_train, y_train, 5)
    algo_lp = algo_lp.fit(X_train, y_train)
    y_predict_lp = pd.DataFrame(algo_lp.predict(X_test), columns=["__label__"])
    print "lp  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_lp), auc(y_test, y_predict_lp))


if __name__ == "__main__":
    run()
