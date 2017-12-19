# coding: utf-8

from http.function import HttpFunction
from http.route import HttpRoute
from http.method import HttpMethod


def http_register(cls):
    HttpFunction.set_func_cls(cls)
    return cls


def http_route(path):
    def func_register(func):
        return HttpRoute(HttpFunction(func), path)
    return func_register


def http_method(method):
    def func_register(func):
        return HttpMethod(HttpFunction(func), method)
    return func_register
