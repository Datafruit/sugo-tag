# coding: utf-8

import numpy as np
import pandas as pd
from tools.operators import listfile
from data.base import BaseData


class BaseDataCsv(BaseData):

    _data_dir = None
    _fields_terminated = '\t'
    _lines_terminated = '\n'
    _pivot = True

    def __init__(self, data=None):
        assert self._data_dir is not None
        super(BaseDataCsv, self).__init__(data)

    def read_data(self):
        return self.read_csv()

    def read_csv(self):
        df = None
        for f in listfile(self._data_dir):
            if f.split('/')[-1].startswith('.'):
                continue
            df_tmp = pd.read_csv(
                f,
                sep=self._fields_terminated,
                header=None,
                names=self._col_all,
                dtype={k: np.str for k in self._col_key},
                engine="python",
                encoding="utf-8"
            )
            df = df.append(df_tmp) if df is not None else df_tmp
        if self._pivot:
            df = pd.pivot_table(df, values=self._col_value, index=self._col_key, columns=self._col_feature)
            df.reset_index(inplace=True)
            df.columns = [c2 if c2 else c1 for c1, c2 in df.columns.tolist()]
            setattr(self, "_col_all", df.columns.tolist())
            setattr(self, "_col_feature", filter(lambda x: x not in self._col_key, df.columns.tolist()))
        return df
