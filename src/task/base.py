# coding: utf-8

import json
import logging
import traceback
import multiprocessing
from multiprocessing.pool import Pool
from luigi import Task, LocalTarget
from luigi.parameter import Parameter
from sklearn.externals.joblib import Parallel, delayed
from conf.env import TASK_TAG_HOME
from conf.env import TASK_LOG_HOME
from conf.env import TASK_DATE


def parallel_func_proxy(cls, parallel_func_name, *args):
    parallel_func = getattr(cls, parallel_func_name)
    return parallel_func(*args)


class NoDaemonProcess(multiprocessing.Process):

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NoDaemonProcessPool(Pool):
    Process = NoDaemonProcess


class BaseTask(Task):

    _tag_dir = TASK_TAG_HOME
    _log_dir = TASK_LOG_HOME
    _task_date = TASK_DATE
    _parallel_task = 1

    def __init__(self, *args, **kwargs):
        super(BaseTask, self).__init__(*args, **kwargs)
        self.clear_target()

    def get_task_name(self):
        return self.__class__.__name__

    def get_log_file(self):
        return "%s/%s.log" % (self._log_dir, self.get_task_name())

    def get_logger(self):
        try:
            self._logger
        except AttributeError:
            self._logger = logging.getLogger(self.get_task_name())
            self._logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s %(name)-15s %(levelname)-8s %(message)s")
            file_handler = logging.FileHandler(self.get_log_file())
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
        return self._logger

    def get_target(self):
        return LocalTarget(self._tag_dir + "/" + self.get_task_name())

    def clear_target(self):
        target = self.get_target()
        if target.exists():
            target.remove()

    def make_target(self):
        target = self.get_target()
        with target.open('w') as fd:
            print >> fd, ""

    def before_run(self):
        pass

    def after_run(self):
        pass

    def output(self):
        return self.get_target()

    def runner(self):
        pass

    def parallel_runner(self, *args):
        pass

    def parallize(self):
        return Parallel(n_jobs=self._parallel_task, verbose=1, backend="multiprocessing")(
            [delayed(parallel_func_proxy)(self.__class__, "parallel_function", *args)
             for args in self.parallel_args_iter()]
        )

    @classmethod
    def parallel_function(cls, *args):
        pass

    def parallel_args_iter(self):
        yield []

    def run(self):
        self.before_run()
        try:
            if self._parallel_task > 1:
                self.parallel_runner(*self.parallize())
            else:
                self.runner()
        except Exception as e:
            self.get_logger().error(traceback.format_exc())
            self.get_logger().error(e.message)
            raise Exception(e)
        self.make_target()
        self.after_run()


class BaseParameter(str):
    """
    {"name": NAME, "value": VALUE}
    """

    def __init__(self, s):
        super(BaseParameter, self).__init__()
        self._param = json.loads(str(s))
        assert "name" in self._param
        assert "value" in self._param

    def __str__(self):
        return json.dumps(self._param)

    def __copy__(self):
        return BaseParameter(json.dumps(self._param))

    @property
    def name(self):
        return self._param["name"]

    @property
    def value(self):
        return self._param["value"]


class BaseTaskWithParams(BaseTask):
    """
    如果设置了 luigi.parameter, luigi.task 会根据变量名和变量位置对应地实例化这个参数值
    """
    param = Parameter()

    def __init__(self, param):
        """
        :param param: obj of BaseParameter or json string
        PS: self._param is copy of param, self.param is copy of self._param.name
        """
        self._param = BaseParameter(param)
        super(BaseTaskWithParams, self).__init__(self._param.name)

    def get_task_name(self):
        base_task_name = super(BaseTaskWithParams, self).get_task_name()
        task_name = base_task_name + "-" + self._param.name
        return task_name
