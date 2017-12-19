# coding: utf-8

from sql.hive_tag import HiveTagSql


class HiveTagCollectorSql(HiveTagSql):

    _table_dst = "user_tag"
    _table_dst_col = "user_tag_col"
    _table_dst_tag = "user_tag_tag"
    _table_dict = "user_tag_dict"
    _pivot_script = "pivot.py"
    _value_to_tag_script = "value_to_tag.py"
    _tags = []  ## list of subclass of hive_tag.HiveTagSql
    _fields = [field for tag in _tags for field in tag._fields.itervalues()]

    @classmethod
    def get_sql_dst(cls):
        key = cls._key
        fields_all = []
        columns_all = []
        first_table = None
        sql_list = []
        for idx, tag in enumerate(cls._tags):
            cur_table = "t{idx}".format(idx=idx)
            first_table = cur_table if idx == 0 else first_table
            fields = [v["name"] for k, v in tag._fields.iteritems()]
            for fd in fields:
                fields_all.append("{t}.{fd} as {fd}".format(t=cur_table, fd=fd))
                columns_all.append(fd)
            sql = """
(select {key},{fields} from {db_src}.{table_src}) {t} {on}
""".format(
                key=key,
                fields=','.join(fields),
                db_src=tag._db_dst,
                table_src=tag._table_dst,
                t=cur_table,
                on="on ({first_table}.{key} = {cur_table}.{key})".format(
                    first_table=first_table,
                    key=key,
                    cur_table=cur_table
                ) if idx > 0 else ""
            )
            sql_list.append(sql)
        return """
drop table if exists {db_dst}.{table_dst};
create table {db_dst}.{table_dst}
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
as select 
{first_table}.{key} as {key},
{fields_all} 
from {sql_list}
;

delete file {udf_home}/python/{pivot_script};
add file {udf_home}/python/{pivot_script};
list files;
drop table if exists {db_dst}.{table_dst_col};
create table {db_dst}.{table_dst_col}
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
as select 
    transform({columns_list})
    using '{python_bin} {pivot_script} "{columns_all}" "{key}" "{columns_pivot}"'
    as ({key}, name, value)
from {db_dst}.{table_dst}
where {key} is not null and lower({key}) <> 'null'
;
""".format(
            db_dst=cls._db_dst,
            table_dst=cls._table_dst,
            table_dst_col=cls._table_dst_col,
            first_table=first_table,
            key=key,
            fields_all=',\n'.join(fields_all),
            sql_list="full join".join(sql_list),
            udf_home=cls._udf_home,
            python_bin=cls._python_bin,
            pivot_script=cls._pivot_script,
            columns_list=','.join([key] + columns_all),
            columns_all='|'.join([key] + columns_all),
            columns_pivot='|'.join(columns_all)
        )

    @classmethod
    def get_sql_dict(cls):
        sql_list = []
        for tag in cls._tags:
            sql = """
select *
from {db_dst}.{table_dict}
""".format(
                db_dst=cls._db_dst,
                table_dict = tag._table_dict
            )
            sql_list.append(sql)
        return """
drop table if exists {db_dst}.{table_dict};
create table {db_dst}.{table_dict}
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
as select 
    t.name as name,
    t.type as type,
    t.tag_name as tag_name,
    t.tag_value as tag_value
from (
{sql_sub}
) t
;

delete file {udf_home}/python/{value_to_tag_script};
add file {udf_home}/python/{value_to_tag_script};
list files;
drop table if exists {db_dst}.{table_dst_tag};
create table {db_dst}.{table_dst_tag}
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
as select
    transform(t1.{key}, t1.name, t1.value, t2.type, t2.tags)
    using '{python_bin} {value_to_tag_script}'
    as ({key}, name, tag_name)
from (
    select {key}, name, value from {db_dst}.{table_dst_col}
) t1 join (
    select 
        name, 
        max(type) as type,
        concat('{{', concat_ws(',', collect_list(concat('"', tag_name, '":"', tag_value, '"'))), '}}') as tags
    from {db_dst}.{table_dict}
    group by name
) t2 on (t1.name = t2.name)
;
""".format(
            db_dst=cls._db_dst,
            table_dict=cls._table_dict,
            sql_sub="union all".join(sql_list),
            table_dst_tag=cls._table_dst_tag,
            key=cls._key,
            udf_home=cls._udf_home,
            python_bin=cls._python_bin,
            value_to_tag_script=cls._value_to_tag_script,
            table_dst_col=cls._table_dst_col
        )
