# coding: utf-8

import os
import sys

if __name__ == "__main__":


    import math
    import numpy as np
    import scipy as sp
    import pandas as pd
    from scipy.sparse import csr_matrix, coo_matrix
    from sklearn.model_selection import KFold

    X = pd.DataFrame(
        data=[1, 1, 1, 0, 0, 1],
    )
    y1 = pd.DataFrame(
        data=[0, 0, 0, 1, 1, 0],
    )
    y2 = pd.DataFrame(
        data=[0.1, 0.2, 0.15, 0.3, 0.4, 0.2],
    )

    print np.apply_along_axis(func1d=lambda x: 1 if x >= 0.3 else 0, axis=1, arr=y2)
    # from sklearn.metrics import roc_curve, f1_score
    # fpr, tpr, threshod = roc_curve(y1, y2)
    # print threshod[np.argmax(np.subtract(tpr, fpr) / np.power(2.0, 0.5))]












