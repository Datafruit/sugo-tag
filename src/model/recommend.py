# coding: utf-8

from sklearn.ensemble import GradientBoostingRegressor
from model.fm import FactorizationMachineAlgorithm
from model.base import BaseModel


class RecommendModel(BaseModel):

    def get_algorithm(self):
        return FactorizationMachineAlgorithm()
        # return GradientBoostingRegressor(
        #     loss="ls",
        #     learning_rate=0.1,
        #     n_estimators=100,
        #     max_depth=3,
        #     min_samples_split=0.01,
        #     min_samples_leaf=0.01,
        #     min_impurity_split=0.01
        # )
