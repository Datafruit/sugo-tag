# coding: utf-8

from http.function import HttpFunction


class HttpMethod(HttpFunction):

    def __init__(self, func, method):
        super(HttpMethod, self).__init__(func)
        func_key = self.get_func_key(self.get_func_name(), method=method)
        self._func_container[func_key] = self

    @classmethod
    def check_method(cls, func_name, method):
        func_key = cls.get_func_key(func_name, method=method)
        return func_key in cls._func_container
