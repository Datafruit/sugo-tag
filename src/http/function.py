# coding: utf-8


class HttpFunction(object):

    _func_container = dict()

    def __init__(self, func):
        if isinstance(func, HttpFunction):
            self._func = func.get_func()
            self._func_name = func.get_func_name()
            self._cls = func.get_cls()
            self._cls_name = func.get_cls_name()
        else:
            self._func = func
            self._func_name = func.__name__
            self._cls = None
            self._cls_name = None

    def __call__(self, *args, **kwargs):
        return self._func(self._cls(), *args, **kwargs)

    @classmethod
    def get_func_key(cls, func_name, *args, **kwargs):
        return cls.__name__ + "/" + "/".join(args) + "/".join(kwargs.values()) + "/" + func_name

    @classmethod
    def set_func_cls(cls, cls_to):
        for route, func in cls._func_container.items():
            if func.get_cls() is None:
                func.set_cls(cls_to)
            elif issubclass(cls_to, func.get_cls()):
                func.set_cls(cls_to)

    def get_func(self):
        return self._func

    def get_func_name(self):
        return self._func_name

    def set_cls(self, cls):
        self._cls = cls
        self._cls_name = cls.__name__

    def get_cls(self):
        return self._cls

    def get_cls_name(self):
        return self._cls_name
