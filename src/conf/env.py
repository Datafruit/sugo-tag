# coding: utf-8

import os
import sys
from datetime import datetime

PROJECT_HOME = os.path.abspath(__file__ + "/../../..")
os.chdir(PROJECT_HOME + "/src")

TASK_DATE = datetime.now().strftime("%Y-%m-%d")
if (len(sys.argv) > 1) and sys.argv[1].isdigit():
    TASK_DATE = datetime.fromtimestamp(int(sys.argv[1]) - 86400).strftime("%Y-%m-%d")
    del sys.argv[1]
TASK_TAG_HOME = PROJECT_HOME + "/task_tag"
TASK_SQL_HOME = PROJECT_HOME + "/task_sql"
TASK_LOG_HOME = PROJECT_HOME + "/log"
TASK_JAR_HOME = PROJECT_HOME + "/jar"
TASK_DATA_HOME = PROJECT_HOME + "/data"
TASK_REPO_HOME = PROJECT_HOME + "/repo"
TASK_UDF_HOME = PROJECT_HOME + "/src/udf"

HIVE_HOME = "/opt/apps/hive_sugo"
HIVE_BIN = HIVE_HOME + "/bin/hive"

PYTHON_HOME = "/usr/local/anaconda2"
PYTHON_BIN = PYTHON_HOME + "/bin/python"

UINDEX_HOST = "192.168.0.217"
UINDEX_PORT_QUERY = "8086"
UINDEX_URL_CREATE = "http://{host}:{port}/druid/hmaster/v1/datasources".format(host=UINDEX_HOST, port=UINDEX_PORT_QUERY)
UINDEX_URL_DELETE = "http://{host}:{port}/druid/hmaster/v1/datasources/force".format(host=UINDEX_HOST, port=UINDEX_PORT_QUERY)
UINDEX_URL_SHOW_TABLES = "http://{host}:{port}/druid/hmaster/v1/datasources/serverview/".format(host=UINDEX_HOST, port=UINDEX_PORT_QUERY)

DRUID_HOST = "192.168.0.217"
DRUID_PORT_QUERY = "8082"
DRUID_URL_PRETTY = "http://{host}:{port}/druid/v2?pretty".format(host=DRUID_HOST, port=DRUID_PORT_QUERY)

WEB_MASTER = "10.10.12.160:8000"
WEB_URL_UPDATE_DIMENSION = "http://%s/api/tag-manger/update-tag-table?prefix={datasource_prefix}&userTagName={datasource_profile}&tagRefName={datasource_profile_tag}" % WEB_MASTER

TAG_SERVER_HOST = "localhost"
TAG_SERVER_PORT = "8888"

USER_KEY = "user_id"

