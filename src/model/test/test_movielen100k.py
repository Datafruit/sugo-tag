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
from model.lr import LogisticRegressionAlgorithm
from model.fm import FactorizationMachineAlgorithm


if __name__ == "__main__":

    data_home = TASK_DATA_HOME + "/algorithm_test/movielen100k"
    df_user = pd.read_csv(
        data_home + "/u.user",
        sep='|',
        names=["user", "age", "gender", "occupation", "zip"]
    )
    discretization_coding(df_user, "user", "age")

    df_user_coding = df_user[["user"]]
    df_user_age = discretization_coding(df_user, "user", "age")
    df_user_coding = df_user_coding.merge(df_user_age, how="inner", on="user")
    df_user_gender = one_hot_coding(df_user, "user", "gender")
    df_user_coding = df_user_coding.merge(df_user_gender, how="inner", on="user")
    df_user_occupation = one_hot_coding(df_user, "user", "occupation")
    df_user_coding = df_user_coding.merge(df_user_occupation, how="inner", on="user")

    df_item = pd.read_csv(
        data_home + "/u.item",
        sep='|',
        names=["item", "title", "release_date", "video_release_date", "url",
               "unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
               "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
               "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
    )
    df_item_coding = df_item[["item", "unknown", "Action", "Adventure", "Animation", "Children",
                              "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir",
                              "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller",
                              "War", "Western"]]

    df_data = pd.read_csv(
        data_home + "/u.data",
        sep='\t',
        names=["user", "item", "rating", "time"]
    )
    df_data = df_data.merge(df_user_coding, how="inner", on="user")
    df_data = df_data.merge(df_item_coding, how="inner", on="item")

    x_columns = filter(lambda x: x not in ("user"), df_user_coding.columns)
    y_columns = filter(lambda x: x not in ("item"), df_item_coding.columns)

    df_train, df_test = train_test_split(df_data, test_size=0.2)

    label = "Action"
    X_train = df_train[x_columns].reset_index(drop=True)
    X_test = df_test[x_columns].reset_index(drop=True)
    y_train = df_train[[label]].reset_index(drop=True)
    y_test = df_test[[label]].reset_index(drop=True)

    ## compare lr, gbdt, fm and deeplearning
    print "############ lr ############"
    algo_lr = LogisticRegressionAlgorithm()
    algo_lr = algo_lr.fit(X_train, y_train)
    y_predict_lr = pd.DataFrame(algo_lr.predict(X_test), columns=["__label__"])
    print "lr  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_lr), auc(y_test, y_predict_lr))

    print "############ gbdt ############"
    algo_gbdt = GradientBoostingRegressor()
    algo_gbdt = algo_gbdt.fit(X_train, y_train)
    y_predict_gbdt = pd.DataFrame(algo_gbdt.predict(X_test), columns=["__label__"])
    print "gbdt: mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_gbdt), auc(y_test, y_predict_gbdt))

    print "############ fm ############"
    algo_fm = FactorizationMachineAlgorithm()
    algo_fm = algo_fm.fit(X_train, y_train)
    y_predict_fm = pd.DataFrame(algo_fm.predict(X_test), columns=["__label__"])
    print "fm  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_fm), auc(y_test, y_predict_fm))

    print "############ dl ############"
    algo_dl = MLPRegressor(
        hidden_layer_sizes=(60, 30, 15),
        activation="logistic",
        solver="adam",
        alpha=0.01,
        random_state=1,
        tol=1e-4
    )
    algo_dl = algo_dl.fit(X_train, y_train)
    y_predict_dl = pd.DataFrame(algo_dl.predict(X_test), columns=["__label__"])
    print "dl  : mse[%.6f], auc[%.6f]" % (mse(y_test, y_predict_dl), auc(y_test, y_predict_dl))

    # y_test["lr"] = y_predict_lr["__label__"]
    # y_test["gbdt"] = y_predict_gbdt["__label__"]
    # y_test["fm"] = y_predict_fm["__label__"]
    # y_test["dl"] = y_predict_dl["__label__"]
    # y_test.sort_values(by=[label], ascending=False).to_csv("/Users/penghuan/Tmp/xxoo", header=True, index=False)
