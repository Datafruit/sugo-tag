# coding: utf-8

from sql.hive import HiveSql
from sql.hive_tag import HiveTagSql


class HiveTagExport(HiveSql):

    _table_dst = None
    _local_dir = None
    _key = HiveTagSql._key

    @classmethod
    def get_sql(cls):
        return """
{args};
insert overwrite local directory '{local_dir}'
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
select {key}, concat(name, '`', tag_name), 1 from {db_dst}.{table_dst}
;
""".format(
            args=cls.get_args(),
            local_dir=cls._local_dir,
            db_dst=cls._db_dst,
            table_dst=cls._table_dst,
            key=cls._key
        )
