# coding: utf-8

from sql.hive_tag import HiveTagSql
from tools.operators import percentile


class IGola_UserBehaviorSql(HiveTagSql):

    _table_dst = "user_behavior"
    _table_dict = "user_behavior_dict"
    _prefix = "behavior"
    _fields = {
        "ticket_domestic_bp":{"name":"%s_ticket_domestic_bp" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.9]},
        "ticket_inter_bp":{"name":"%s_ticket_inter_bp" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.5,0.7,0.9]}
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
    t.ticket_domestic_bp as {ticket_domestic_bp_name},
    t.ticket_inter_bp as {ticket_inter_bp_name}
from (
    select 
        a2.userid as uid,
        sum(case when lower(a1.suppliercode) = 'biqu' then datediff(from_unixtime(unix_timestamp(substr(a1.traveldate, 0, 8), 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(cast(substr(a1.orderdatetime, 0, 10) as bigint), 'yyyy-MM-dd')) else null end) / sum(case when lower(a1.suppliercode) = 'biqu' then 1 else null end) as ticket_domestic_bp,
        sum(case when lower(a1.suppliercode) != 'biqu' then datediff(from_unixtime(unix_timestamp(substr(a1.traveldate, 0, 8), 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(cast(substr(a1.orderdatetime, 0, 10) as bigint), 'yyyy-MM-dd')) else null end) / sum(case when lower(a1.suppliercode) != 'biqu' then 1 else null end) as ticket_inter_bp
    from (
        select * from {db_src}.flights_order
    ) a1 join (
        -- 只取成功的订单
        select userid, orderid
        from {db_src}.flights_magic_order
        where orderstatus = "SUCCESS"
        group by userid, orderid
    ) a2 on (a1.orderid = a2.orderid)
    group by a2.userid
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