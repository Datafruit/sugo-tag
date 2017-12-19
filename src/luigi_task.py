# coding: utf-8

import conf.env
import sys
from luigi.cmdline import luigi_run

if __name__ == "__main__":
    ret_code = luigi_run()
    sys.exit(ret_code)
