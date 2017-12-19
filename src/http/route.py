# coding: utf-8

from http.function import HttpFunction


class HttpRoute(HttpFunction):

    def __init__(self, func, path):
        super(HttpRoute, self).__init__(func)
        func_key = self.get_func_key(self.get_func_name(), path=path)
        if func_key not in self._func_container:
            self._func_container[func_key] = self
        else:
            raise Exception("route [%s] has been registered" % func_key)

    @classmethod
    def parse_route(cls, route):
        elems = route.split('/')
        path = '/'.join(elems[:len(elems)-1])
        func_name = elems[-1]
        return func_name, path

    @classmethod
    def check_route(cls, route):
        func_name, path = cls.parse_route(route)
        func_key = cls.get_func_key(func_name, path=path)
        return func_key in cls._func_container

    @classmethod
    def search_func(cls, route):
        func_name, path = cls.parse_route(route)
        func_key = cls.get_func_key(func_name, path=path)
        return cls._func_container[func_key]
