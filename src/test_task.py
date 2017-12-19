# coding: utf-8


if __name__ == "__main__":

    import itertools
    import conf.env
    import thread
    import multiprocessing
    from multiprocessing.pool import Pool
    from luigi.retcodes import run_with_retcodes


    luigi_argv = {
        "--module": ["task.test", "Task0"],
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
    argv = list(itertools.chain(*[[k] + v for k, v in luigi_argv.iteritems()]))

    ret_code = run_with_retcodes(argv)
    print "##############", ret_code


