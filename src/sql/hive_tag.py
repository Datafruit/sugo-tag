# coding: utf-8

from conf.env import USER_KEY
from sql.hive import HiveSql


class HiveTagSql(HiveSql):

    _db_src = "sugo_sqoop"
    _table_dst = None
    _table_dict = None
    _key = USER_KEY
    _prefix = None
    _fields = {}
    _jar = "tag-1.0-SNAPSHOT-jar-with-dependencies.jar"
    _cp_segment = "io.druid.hive.udf.Segment"
    _udf_segment = "my_segment"

    @classmethod
    def args_add(cls):
        return [
            "add jar {jar_dir}/{jar};".format(jar_dir=cls._jar_dir, jar=cls._jar),
        ]

    @classmethod
    def get_sql(cls):
        return """
{args};
{sql_dst};
{sql_dict};
""".format(
            args=cls.get_args(),
            sql_dst=cls.get_sql_dst(),
            sql_dict=cls.get_sql_dict()
        )

    @classmethod
    def get_sql_dst(cls):
        raise NotImplementedError

    @classmethod
    def get_sql_dict(cls):
        sql_list = []
        for k, v in cls._fields.iteritems():
            fd_name = v["name"]
            fd_attr = v["attribute"]
            fd_type = v["type"]
            sql = None
            if fd_attr == 2:
                sql = """
select 
    "{field_name}" as name,
    {field_attr} as type,
    {field_name} as tag_name,
    {field_name} as tag_value
from {db_dst}.{table_dst}
group by {field_name}
""".format(
                    db_dst=cls._db_dst,
                    table_dst=cls._table_dst,
                    table_dict=cls._table_dict,
                    field_name=fd_name,
                    field_attr=fd_attr
                )
            elif fd_attr == 1:
                fd_segment = v["segment"] if v.has_key("segment") else None
                fd_percentile = v["percentile"]if v.has_key("percentile") else None
                if fd_segment:
                    sql = """
select 
    "{field_name}" as name,
    {field_attr} as type,
    tag["name"] as tag_name,
    tag["value"] as tag_value
from (
    select {udf_segment}(array({segment})) as tags
    from {db_dst}.{table_dst}
    where {field_name} is not null
    limit 1
) t1 lateral view explode(t1.tags) t_tag as tag
""".format(
                        udf_segment=cls._udf_segment,
                        segment=','.join([str(v) for v in fd_segment]),
                        db_dst=cls._db_dst,
                        table_dst=cls._table_dst,
                        field_name=fd_name,
                        field_attr=fd_attr,
                    )
                elif fd_percentile:
                    sql = """
select 
    "{field_name}" as name,
    {field_attr} as type,
    tag["name"] as tag_name,
    tag["value"] as tag_value
from (
    select {udf_segment}(t2.pt_new) as tags
    from (
        select sort_array(collect_list(cast(pt_one as {field_type}))) as pt_new
        from (
            select percentile(cast({field_name} as bigint), array({field_percentile})) as pt
            from {db_dst}.{table_dst}
            where {field_name} is not null
        ) t1 lateral view explode(t1.pt) t_pt as pt_one
    ) t2
) t3 lateral view explode(t3.tags) t_tag as tag
""".format(
                        udf_segment=cls._udf_segment,
                        db_dst=cls._db_dst,
                        table_dst=cls._table_dst,
                        table_dict=cls._table_dict,
                        field_name=fd_name,
                        field_attr=fd_attr,
                        field_type=fd_type,
                        field_percentile=", ".join(["%.2f" % pt for pt in fd_percentile])
                    )
            if sql:
                sql_list.append(sql)

        return """
drop temporary function if exists {udf_segment};
create temporary function {udf_segment} as "{cp_segment}";
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
{sub_sql}
) t
;
""".format(
            udf_segment=cls._udf_segment,
            cp_segment=cls._cp_segment,
            db_dst=cls._db_dst,
            table_dst=cls._table_dst,
            table_dict=cls._table_dict,
            sub_sql="union all".join(sql_list)
        )
