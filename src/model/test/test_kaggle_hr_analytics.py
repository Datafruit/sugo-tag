# coding: utf-8

from conf.env import TASK_DATA_HOME
from tools.machine_learning import one_hot_coding
from tools.machine_learning import discretization_coding
from tools.machine_learning import mse
from tools.machine_learning import auc
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor
from model.base import kfold_cross_validation
from model.lr import LogisticRegressionAlgorithm
from model.fm import FactorizationMachineAlgorithm


def run():
    data_home = TASK_DATA_HOME + "/algorithm_test/kaggle/hr_analytics"
    df = pd.read_csv(data_home + "/hr.csv")
    df.reset_index(inplace=True)
    id = "index"
    features_continuous = [
        "satisfaction_level",
        "last_evaluation",
        "number_project",
        "average_montly_hours",
        "time_spend_company",
    ]
    features_discrete = [
        "Work_accident",
        "promotion_last_5years",
        "sales",
        "salary"
    ]
    df["__label__"] = df["left"].apply(lambda x: 1 if int(x) == 1 else 0)
    label = "__label__"
    df_coding = df[[id, label]]
    for feature in features_continuous:
        df_coding_f = discretization_coding(df, id, feature)
        df_coding = df_coding.merge(df_coding_f, how="inner", on=id)
    for feature in features_discrete:
        df_coding_f = one_hot_coding(df, id, feature)
        df_coding = df_coding.merge(df_coding_f, how="inner", on=id)

    x_columns = filter(lambda x: x not in (id, label), df_coding.columns)
    y_columns = [label]
    df_train, df_test = train_test_split(df_coding, test_size=0.2)
    X_train = df_train[x_columns].reset_index(drop=True)
    X_test = df_test[x_columns].reset_index(drop=True)
    y_train = df_train[[label]].reset_index(drop=True)
    y_test = df_test[[label]].reset_index(drop=True)


    ## compare lr, gbdt, fm and deeplearning
    print "############ lr ############"
    algo_lr = LogisticRegressionAlgorithm()
    mse_lr, auc_lr, threshold_lr = kfold_cross_validation(algo_lr, X_train, y_train, 5)
    algo_lr = algo_lr.fit(X_train, y_train)
    y_predict_lr = pd.DataFrame(algo_lr.predict(X_test), columns=["__label__"])
    y_predict_lr["__class__"] = y_predict_lr["__label__"].apply(lambda x: 1 if x >= threshold_lr else 0)
    print "lr  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_lr["__label__"]), auc(y_test, y_predict_lr["__label__"]))

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
    #
    # print "############ dl ############"
    # algo_dl = MLPRegressor(
    #     hidden_layer_sizes=(60, 30, 15),
    #     activation="logistic",
    #     solver="adam",
    #     alpha=0.01,
    #     random_state=1,
    #     tol=1e-4
    # )
    # algo_dl = algo_dl.fit(X_train, y_train)
    # y_predict_dl = pd.DataFrame(algo_dl.predict(X_test), columns=["__label__"])
    # print "dl  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_dl), auc(y_test, y_predict_dl))

    # y_test["lr"] = y_predict_lr["__label__"]
    # y_test["gbdt"] = y_predict_gbdt["__label__"]
    # y_test["fm"] = y_predict_fm["__label__"]
    # y_test["dl"] = y_predict_dl["__label__"]
    # y_test.sort_values(by=[label], ascending=False).to_csv("/Users/penghuan/Tmp/xxoo", header=True, index=False)

if __name__ == "__main__":

    run()

