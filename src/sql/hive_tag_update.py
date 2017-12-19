# coding: utf-8

from conf.env import UINDEX_HOST
from conf.env import UINDEX_PORT_QUERY
from sql.hive_tag_collector import HiveTagCollectorSql
from sql.hive_tag import HiveTagSql


class HiveTagUpdateSql(HiveTagSql):

    _uindex_master = "%s:%s" % (UINDEX_HOST, UINDEX_PORT_QUERY)
    _cp_uindex_add = "io.druid.hive.transform.UIndexAdd"

    _table_dst = HiveTagCollectorSql._table_dst
    _table_dict = HiveTagCollectorSql._table_dict
    _hive_tag_collector_sql_cls = None
    _uindex_dst = None
    _uindex_dict = None

    @classmethod
    def args_add(cls):
        return super(HiveTagUpdateSql, cls).args_add() + [
            "set mapreduce.map.memory.mb=4096;",
            "set mapreduce.reduce.memory.mb=8192;",
            "set mapreduce.map.java.opts=-Xmx3072m;",
            "set mapreduce.reduce.java.opts=-Xmx6144m;"
        ]

    @classmethod
    def get_sql_dst(cls):
        field_list = []
        field_name_list = [cls._key]
        for fd_info in cls._hive_tag_collector_sql_cls._fields:
            fd_name = fd_info["name"]
            fd_type = fd_info["type"]
            field_list.append("cast({fd_name} as {fd_type})".format(fd_name=fd_name, fd_type=fd_type))
            field_name_list.append(fd_name)
        return """
select '{db_dst}', '{table_dst}', count(1)
from (
    select transform(
{key},
{fields}
    )
    using "java -Xmx64m -Xms64m -cp {jar} {classpath} {master} {data_source} {fields_name}" as (fd)
    from {db_dst}.{table_dst}
    where {key} is not null
) t
;
""".format(
            jar=cls._jar,
            classpath=cls._cp_uindex_add,
            master=cls._uindex_master,
            data_source=cls._uindex_dst,
            db_dst=cls._db_dst,
            table_dst=cls._table_dst,
            key=cls._key,
            fields=',\n'.join(field_list),
            fields_name='`'.join(field_name_list)
        )

    @classmethod
    def get_sql_dict(cls):
        return """
select '{db_dst}', '{table_dict}', count(1)
from (
    select transform(
        concat(name, '_', tag_name), 
        name, 
        type, 
        tag_name, 
        tag_value
    ) using "java -Xmx64m -Xms64m -cp {jar} {classpath} {master} {data_source} {fields_name}" as (fd)
    from {db_dst}.{table_dict}
    where tag_name is not null and length(tag_name) > 0
) t
;
""".format(
            jar=cls._jar,
            classpath=cls._cp_uindex_add,
            master=cls._uindex_master,
            data_source=cls._uindex_dict,
            db_dst=cls._db_dst,
            table_dict=cls._table_dict,
            fields_name='`'.join(["key", "name", "type", "tag_name", "tag_value"])
        )