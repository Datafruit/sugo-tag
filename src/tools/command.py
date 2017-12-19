# coding: utf-8

import sys
import subprocess


class Command(object):

    @staticmethod
    def run(args, log_file=None):
        if log_file:
            with open(log_file, 'a') as fd:
                subprocess.check_call(args, stdout=fd, stderr=fd, shell=True)
        else:
            subprocess.check_call(args, stdout=sys.stdout, stderr=sys.stderr, shell=True)

    @staticmethod
    def run_output(args, log_file=None):
        if log_file:
            with open(log_file, 'a') as fd:
                return subprocess.check_output(args, stderr=fd, shell=True)
        else:
            return subprocess.check_output(args, stderr=sys.stderr, shell=True)
