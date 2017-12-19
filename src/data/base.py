# coding: utf-8

import copy
import pandas as pd


class BaseData(object):

    _col_all = []
    _col_key = []
    _col_feature = []
    _col_value = []  ## must has value column if _pivot = True
    _col_label = "__label__"
    _pivot = False

    def __init__(self, data=None, *args, **kwargs):
        assert self._col_all
        assert self._col_key
        # assert self._col_feature
        assert self._col_value if self._pivot else True
        self._data = data if data is not None else self.read_data()

    def read_data(self):
        raise NotImplementedError

    def get_data(self):
        return self._data[self._col_all]

    def get_shape(self):
        return self._data.shape

    def set_label(self, label):
        self._data[self._col_label] = label
        self._col_all.append(self._col_label)

    def intersection(self, data, on, inplace=False):
        data_inter = self._data.merge(data.get_data(), how="inner", on=on)
        data_inter.reset_index(inplace=True)
        if inplace:
            self._data = data_inter
        else:
            new_data = self.__class__(data_inter)
            for attr in dir(self.__class__):
                if not callable(getattr(self, attr)) and not attr.startswith("__"):
                    setattr(new_data, attr, copy.deepcopy(getattr(self, attr)))
            return new_data

    def difference(self, data, on, inplace=False):
        data_merge = self._data.merge(data.get_data(), how="left", on=on, indicator=True)
        data_idx = data_merge["_merge"] == "left_only"
        data_diff = self._data[data_idx]
        data_diff.reset_index(inplace=True)
        if inplace:
            self._data = data_diff
        else:
            new_data = self.__class__(data_diff)
            for attr in dir(self.__class__):
                if not callable(getattr(self, attr)) and not attr.startswith("__"):
                    setattr(new_data, attr, copy.deepcopy(getattr(self, attr)))
            return new_data

