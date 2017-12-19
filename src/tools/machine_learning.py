# coding: utf-8

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, roc_auc_score, log_loss


def one_hot_coding(df, key, feature):
    df_coding = df[[key, feature]]
    df_coding["value"] = 1
    df_coding = pd.pivot_table(df_coding, "value", key, feature, fill_value=0)
    df_coding.reset_index(inplace=True)
    df_coding.rename(columns=lambda x: feature+"`"+str(x) if x != key else str(x), inplace=True)
    return df_coding


def discretization_coding(df, key, feature, quantile=10):
    df_coding = df[[key, feature]]
    quantile = df_coding[[feature]] \
        .quantile([float(i)/quantile for i in range(1, quantile)]) \
        .drop_duplicates(subset=feature)
    interval = [(round(idx, 2), round(row[feature], 6)) for idx, row in quantile.iterrows()]
    def find_interval(x):
        for percent, threshold in interval:
            if x <= threshold:
                return percent
        return 1.0
    df_coding["percent"] = df_coding[feature].apply(func=find_interval)
    df_coding["value"] = 1
    df_coding = pd.pivot_table(df_coding[[key, "percent", "value"]], "value", key, "percent", fill_value=0)
    df_coding.reset_index(inplace=True)
    df_coding.rename(columns=lambda x: feature+"`"+str(x) if x != key else x, inplace=True)
    return df_coding


def mse(y1, y2):
    return mean_squared_error(y1, y2)


def logloss(y_true, y_pred):
    return log_loss(y_true, y_pred)


def auc(y_true, y_pred):
    return roc_auc_score(y_true, y_pred)


