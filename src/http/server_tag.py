# coding: utf-8

import os
import sys
import time
import json
import copy
import threading
import shutil
import itertools

from conf.env import TASK_LOG_HOME
from luigi.retcodes import run_with_retcodes
from task.base import NoDaemonProcessPool


def task_runner(task_name, log_file, argv):
    task_message = {
        0: "运行成功",
        1: "未知异常",
        2: "数据缺失",
        3: "任务失败",
        4: "有重复的任务正在运行",
        5: "任务计划失败",
        6: "任务未运行",
        98: "其它错误",
        99: "未知错误"
    }
    log_fd = open(log_file, 'a')
    sys.stdout = log_fd
    sys.stderr = log_fd
    now = time.time()
    year, month, day, hh, mm, ss, x, y, z = time.localtime(now)
    ts = "%02d/%02d/%04d %02d:%02d:%02d" % (day, month, year, hh, mm, ss)
    print >> log_fd, ts, task_name, argv
    try:
        ret_code = int(run_with_retcodes(argv))
        ret_msg = task_message[ret_code] if ret_code in task_message else task_message[99]
    except Exception as e:
        ret_code = 98
        ret_msg = e.message
    return json.dumps({"code": ret_code, "message": ret_msg})


class TagServer(object):

    _process_pool = NoDaemonProcessPool(processes=1)
    _repository = None  ## must be inherited
    _tag_task_module = []  ## must be inherited
    _task_record = {}  ## must be inherited
    _task_log_dir = TASK_LOG_HOME
    _luigi_argv = {
        "--module": [],
        "--local-scheduler": [],
        # "--scheduler-host": ["localhost"],
        # "--scheduler-port": ["8082"],
        "--workers": ["4"],
        "--worker-keep-alive": [],
        "--worker-wait-interval": ["3"],
        "--logging-conf-file": ["conf/logging.cfg"],
        "--scheduler-retry-count": ["1"],
        "--scheduler-retry-delay": ["3"],
        "--no-lock": [],

        "--retcode-unhandled-exception": ["1"],
        "--retcode-missing-data": ["2"],
        "--retcode-task-failed": ["3"],
        "--retcode-already-running": ["4"],
        "--retcode-scheduling-error": ["5"],
        "--retcode-not-run": ["6"],
    }
    _task_status = {
        "running": "RUNNING",
        "pending": "PENDING",
        "failed": "FAILED",
        "success": "SUCCESS"
    }

    _task_response_status = {
        "PENDING": 0,
        "RUNNING": 1,
        "SUCCESS": 2,
        "FAILED": 3
    }
    _task_response_code = {
        "SUCCESS": 0,
        "RUNNING": 0,
        "PENDING": 0,
        "FAILED": 1000,
        "group_none": 1001,
        "group_time_out": 1002,
        "group_full": 1003
    }
    _task_response = {
        "status": None,
        "lastComputeTime": None,
        "code": None,
        "message": None
    }

    @classmethod
    def detection(cls):
        now = time.time()
        year, month, day, hh, mm, ss, x, y, z = time.localtime(now)
        ts = "%02d/%02d/%04d %02d:%02d:%02d" % (day, month, year, hh, mm, ss)
        sts = {v: 0 for k, v in cls._task_status.items()}
        for task_name, res in cls._task_record.iteritems():
            if res is not None and res.ready():
                ret = json.loads(res.get())
                ret_code = ret["code"]
                ret_msg = {"message": ret["message"], "time": int(now)}
                if res.successful():
                    if ret_code == 0:
                        cls.make_task_status(task_name, cls._task_status["success"])
                    else:
                        cls.make_task_status(task_name, cls._task_status["failed"])
                else:
                    cls.make_task_status(task_name, cls._task_status["failed"])
                cls.write_repository_msg_file(task_name, json.dumps(ret_msg))
                cls._task_record[task_name] = None
            task_status = cls.get_task_status(task_name)
            sts[task_status] = sts[task_status] + 1 if task_status in sts else 1
        print >> sys.stderr, "%s  %s  check %s" % (ts, cls.__name__, ', '.join(["%s:%d" % (status, cnt) for status, cnt in sts.iteritems()]))
        threading.Timer(5, cls.detection).start()

    @classmethod
    def get_all_task(cls):
        for p in os.listdir(cls._repository):
            if os.path.isdir(os.path.join(cls._repository, p)):
                task_name = p
                cls._task_record[task_name] = None
                task_status = cls.get_task_status(task_name)
                if task_status not in (cls._task_status["success"], cls._task_status["failed"]):
                    cls.make_task_status(task_name, cls._task_status["failed"])

    def get_task_params(self, task_name, **kwargs):
        argv = copy.deepcopy(self._luigi_argv)
        argv["--module"] = self._tag_task_module
        argv = list(itertools.chain(*[[k] + v for k, v in argv.iteritems()]))
        if kwargs:
            param = {"name": task_name, "value": kwargs}
            argv = argv + ["--param", json.dumps(param)]
        return argv

    @classmethod
    def get_repository(cls, task_name):
        return cls._repository + "/" + task_name

    @classmethod
    def new_repository(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        if os.path.exists(repo_dir):
            shutil.rmtree(repo_dir, ignore_errors=True)
        os.mkdir(repo_dir)
        return repo_dir

    @classmethod
    def delete_repository(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        if os.path.exists(repo_dir):
            shutil.rmtree(repo_dir, ignore_errors=True)

    @classmethod
    def get_repository_data_file(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        return repo_dir + "/repo.data"

    @classmethod
    def get_repository_msg_file(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        return repo_dir + "/repo.msg"

    @classmethod
    def write_repository_msg_file(cls, task_name, msg):
        repo_msg_file = cls.get_repository_msg_file(task_name)
        msg = msg.encode("utf-8") if isinstance(msg, unicode) else msg
        with open(repo_msg_file, 'w') as fd:
            print >> fd, msg

    @classmethod
    def read_repository_msg_file(cls, task_name):
        repo_msg_file = cls.get_repository_msg_file(task_name)
        with open(repo_msg_file, 'r') as fd:
            return fd.read()

    @classmethod
    def read_repository_data_file(cls, task_name):
        repo_data_file = cls.get_repository_data_file(task_name)
        with open(repo_data_file, 'r') as fd:
            return fd.read()

    @classmethod
    def read_task_last_compute_time(cls, task_name):
        if cls.get_task_status(task_name) != cls._task_status["success"]:
            return None
        data = json.loads(cls.read_repository_msg_file(task_name))
        return data["time"]

    @classmethod
    def clear_task_status(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        for f_name in os.listdir(repo_dir):
            f_path = os.path.join(repo_dir, f_name)
            if os.path.isfile(f_path) and f_name in cls._task_status.values():
                os.remove(f_path)

    @classmethod
    def make_task_status(cls, task_name, status):
        if status not in cls._task_status.values():
            return
        cls.clear_task_status(task_name)
        repo_dir = cls.get_repository(task_name)
        f_path = os.path.join(repo_dir, status)
        open(f_path, 'w').close()

    @classmethod
    def get_task_status(cls, task_name):
        repo_dir = cls.get_repository(task_name)
        if os.path.exists(repo_dir):
            for f_name in os.listdir(repo_dir):
                f_path = os.path.join(repo_dir, f_name)
                if os.path.isfile(f_path) and f_name in cls._task_status.values():
                    return f_name
        return None

    def task_run(self, task_name, argv):
        ## check if task queue is too long
        if len(self._process_pool._cache) > 10:
            task_status = self._task_status["pending"]
        else:
            ## check if task is running
            task_status = self.get_task_status(task_name)
            if task_status not in (self._task_status["running"], self._task_status["pending"]):
                ## recreate repository and make status pending
                task_status = self._task_status["pending"]
                self.new_repository(task_name)
                self.make_task_status(task_name, task_status)
                ## run task
                log_file = self._task_log_dir + "/%s.log" % self.__class__.__name__
                res = self._process_pool.apply_async(task_runner, (task_name, log_file, argv, ))
                task_status = self._task_status["running"]
                self.make_task_status(task_name, task_status)
                self._task_record[task_name] = res
        resp = copy.deepcopy(self._task_response)
        resp["status"] = self._task_response_status[task_status]
        resp["code"] = self._task_response_code[task_status]
        return json.dumps(resp)

    def task_status(self, task_name):
        resp = copy.deepcopy(self._task_response)
        task_status = self.get_task_status(task_name)

        resp["status"] = self._task_response_status[task_status]
        resp["code"] = self._task_response_code[task_status]
        resp["lastComputeTime"] = self.read_task_last_compute_time(task_name)
        return json.dumps(resp)

    def task_query(self, task_name):
        status = self.get_task_status(task_name)
        if status is None:
            return '{}'
        data = json.loads(self.read_repository_data_file(task_name))
        data["lastComputeTime"] = self.read_task_last_compute_time(task_name)
        return json.dumps(data)

    def task_delete(self, task_name):
        status = self.get_task_status(task_name)
        if status == self._task_status["running"]:
            return "task is running"
        self.delete_repository(task_name)
        if task_name in self._task_record:
            del self._task_record[task_name]
        return "success"
