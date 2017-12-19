#coding: utf-8

from sql.hive_tag import HiveTagSql
from tools.operators import percentile


class IGola_UserProfileSql(HiveTagSql):

    _table_dst = "user_profile"
    _table_dict = "user_profile_dict"
    _prefix = "profile"
    _fields = {
        "gender":{"name":"%s_gender" % _prefix, "attribute":2, "type":"string"},
        # "age":{"name":"%s_age" % _prefix, "attribute":1, "type":"int", "percentile":percentile(10)},
        "reg_bp":{"name":"%s_reg_bp" % _prefix, "attribute":1, "type":"int", "segment":[30,90,180,360]},
        "login_bp":{"name":"%s_login_bp" % _prefix, "attribute":1, "type":"int", "segment":[30,90,180,360]},
        "channel":{"name":"%s_channel" % _prefix, "attribute":2, "type":"string"},
        "platform":{"name":"%s_platform" % _prefix, "attribute":2, "type":"string"},
    }

    @classmethod
    def get_sql_dst(cls):
        return """
drop table if exists {db_dst}.{table_dst};
create table {db_dst}.{table_dst} 
row format delimited fields terminated by '\\t' lines terminated by '\\n'
stored as textfile
as select 
    t.uid as {key},
    t.gender as {gender_name},
    t.reg_bp as {reg_bp_name},
    t.login_bp as {login_bp_name},
    t.channel as {channel_name},
    t.platform as {platform_name}
from (
    select
        t1.guid as uid,
        case when t1.gender = '0' then 'female' when t1.gender = '1' then 'male' else NULL end as gender,
        case when t1.birthday is not null and lower(t1.birthday) != 'null' then datediff('{date}', t1.birthday) / 365 else NULL end as age,
        case when t1.created_date_time is not null and lower(t1.created_date_time) != 'null' then datediff('{date}', substr(t1.created_date_time, 0, 10)) else NULL end as reg_bp,
        case when t1.last_login_dateTime is not null and lower(t1.last_login_dateTime) != 'null' then datediff('{date}', substr(t1.last_login_dateTime, 0, 10)) else NULL end as login_bp,
        case 
            when t1.channel is not null and lower(t1.channel) != 'null' and substr(t1.channel, 0, 4) != 'http' then t1.channel 
            when t1.channel is not null and lower(t1.channel) != 'null' and substr(t1.channel, 0, 4) = 'http' then 'other'
            else NULL 
        end as channel,
        case 
            when t1.platform = '0' then 'Android'
            when t1.platform = '1' then 'IOS'
            when t1.platform = '2' then 'PC'
            else NULL
        end as platform
    from (
        select * from {db_src}.member
    ) t1 join (
        -- 只取最新的一条数据
        select guid, max(cast(id as bigint)) as id
        from {db_src}.member
        group by guid 
    ) t2 on (cast(t1.id as bigint) = t2.id)
) t
;
""".format(
            db_src=cls._db_src,
            db_dst=cls._db_dst,
            table_dst=cls._table_dst,
            key=cls._key,
            prefix=cls._prefix,
            date=cls._task_date,
            **{"%s_%s"%(k,s):v[s] for k, v in cls._fields.iteritems() for s in v.iterkeys()}
        )
