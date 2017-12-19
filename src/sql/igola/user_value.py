# coding: utf-8

from sql.hive_tag import HiveTagSql


class IGola_UserValueSql(HiveTagSql):

    _table_dst = "user_value"
    _table_dict = "user_value_dict"
    _prefix = "value"
    _fields = {
        "order_bp":{"name":"%s_order_bp" % _prefix, "attribute":1, "type":"int", "segment":[30,90,180,360]},

        "u_price_avg":{"name":"%s_u_price_avg" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "t_price_avg":{"name":"%s_t_price_avg" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.5,0.7,0.9]},
        "order_num":{"name":"%s_order_num" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money":{"name":"%s_money" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.7,0.9]},
        "order_coupon_num":{"name":"%s_order_coupon_num" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money":{"name":"%s_coupon_money" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_90":{"name":"%s_u_price_avg_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_90":{"name":"%s_t_price_avg_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.4,0.6,0.9]},
        "order_num_90":{"name":"%s_order_num_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_90":{"name":"%s_money_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_coupon_num_90":{"name":"%s_order_coupon_num_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_90":{"name":"%s_coupon_money_90" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_180":{"name":"%s_u_price_avg_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_180":{"name":"%s_t_price_avg_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_num_180":{"name":"%s_order_num_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_180":{"name":"%s_money_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "order_coupon_num_180":{"name":"%s_order_coupon_num_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_180":{"name":"%s_coupon_money_180" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},
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
    
    t.order_bp as {order_bp_name},
    t.u_price_avg as {u_price_avg_name},
    t.t_price_avg as {t_price_avg_name},
    t.order_num as {order_num_name},
    t.money as {money_name},
    t.order_coupon_num as {order_coupon_num_name},
    t.coupon_money as {coupon_money_name},
    
    t.u_price_avg_90 as {u_price_avg_90_name},
    t.t_price_avg_90 as {t_price_avg_90_name},
    t.order_num_90 as {order_num_90_name},
    t.money_90 as {money_90_name},
    t.order_coupon_num_90 as {order_coupon_num_90_name},
    t.coupon_money_90 as {coupon_money_90_name},
    
    t.u_price_avg_180 as {u_price_avg_180_name},
    t.t_price_avg_180 as {t_price_avg_180_name},
    t.order_num_180 as {order_num_180_name},
    t.money_180 as {money_180_name},
    t.order_coupon_num_180 as {order_coupon_num_180_name},
    t.coupon_money_180 as {coupon_money_180_name}
from (
    select 
        a2.userid as uid,
        
        min(days) as order_bp,
        sum(a1.totalprice) / count(1) as u_price_avg,
        sum(a1.totalprice) / sum(a3.ticket_num) as t_price_avg,
        sum(1) as order_num,
        sum(a1.totalprice) as money,
        sum(a2.coupon_num) as order_coupon_num,
        sum(a2.coupon_money) as coupon_money,
        
        sum(case when a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.days <= 90 then 1 else null end) as u_price_avg_90,
        sum(case when a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.days <= 90 then a3.ticket_num else null end) as t_price_avg_90,
        sum(case when a1.days <= 90 then 1 else null end) as order_num_90,
        sum(case when a1.days <= 90 then a1.totalprice else null end) as money_90,
        sum(case when a1.days <= 90 then a2.coupon_num else null end) as order_coupon_num_90,
        sum(case when a1.days <= 90 then a2.coupon_money else null end) as coupon_money_90,
        
        sum(case when a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.days <= 180 then 1 else null end) as u_price_avg_180,
        sum(case when a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.days <= 180 then a3.ticket_num else null end) as t_price_avg_180,
        sum(case when a1.days <= 180 then 1 else null end) as order_num_180,
        sum(case when a1.days <= 180 then a1.totalprice else null end) as money_180,
        sum(case when a1.days <= 180 then a2.coupon_num else null end) as order_coupon_num_180,
        sum(case when a1.days <= 180 then a2.coupon_money else null end) as coupon_money_180
    from (
        select 
            *,
            case when lower(suppliercode) = 'biqu' then 0 else 1 end as is_inter,
            datediff('{date}', from_unixtime(cast(substr(orderdatetime, 0, 10) as bigint), 'yyyy-MM-dd')) as days
        from {db_src}.flights_order
    ) a1 join (
        -- 只取成功的订单, 顺便把uid也取出来, 再顺便把优惠券信息取出来
        select 
            userid, 
            orderid,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then 1 else null end) as coupon_num,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then coupondiscount else null end) as coupon_money
        from {db_src}.flights_magic_order
        where orderstatus = "SUCCESS"
        group by userid, orderid
    ) a2 on (a1.orderid = a2.orderid) join (
        -- 每个订单的票数
        select 
            orderid, 
            count(distinct orderitemid) as ticket_num
        from {db_src}.flights_order_item
        where orderid is not null and orderid <> 'null'
        group by orderid
    ) a3 on (a1.orderid = a3.orderid)
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


class IGola_UserValue2Sql(HiveTagSql):

    _table_dst = "user_value2"
    _table_dict = "user_value2_dict"
    _prefix = "value"
    _fields = {
        "order_bp_inter":{"name":"%s_order_bp_inter" % _prefix, "attribute":1, "type":"int", "segment":[30,90,180,360]},

        "u_price_avg_inter":{"name":"%s_u_price_avg_inter" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "t_price_avg_inter":{"name":"%s_t_price_avg_inter" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.5,0.7,0.9]},
        "order_num_inter":{"name":"%s_order_num_inter" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_inter":{"name":"%s_money_inter" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.7,0.9]},
        "order_coupon_num_inter":{"name":"%s_order_coupon_num_inter" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_inter":{"name":"%s_coupon_money_inter" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_inter_90":{"name":"%s_u_price_avg_inter_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_inter_90":{"name":"%s_t_price_avg_inter_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.4,0.6,0.9]},
        "order_num_inter_90":{"name":"%s_order_num_inter_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_inter_90":{"name":"%s_money_inter_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_coupon_num_inter_90":{"name":"%s_order_coupon_num_inter_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_inter_90":{"name":"%s_coupon_money_inter_90" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_inter_180":{"name":"%s_u_price_avg_inter_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_inter_180":{"name":"%s_t_price_avg_inter_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_num_inter_180":{"name":"%s_order_num_inter_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_inter_180":{"name":"%s_money_inter_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "order_coupon_num_inter_180":{"name":"%s_order_coupon_num_inter_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_inter_180":{"name":"%s_coupon_money_inter_180" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},
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

    t.order_bp_inter as {order_bp_inter_name},
    t.u_price_avg_inter as {u_price_avg_inter_name},
    t.t_price_avg_inter as {t_price_avg_inter_name},
    t.order_num_inter as {order_num_inter_name},
    t.money_inter as {money_inter_name},
    t.order_coupon_num_inter as {order_coupon_num_inter_name},
    t.coupon_money_inter as {coupon_money_inter_name},
    
    t.u_price_avg_inter_90 as {u_price_avg_inter_90_name},
    t.t_price_avg_inter_90 as {t_price_avg_inter_90_name},
    t.order_num_inter_90 as {order_num_inter_90_name},
    t.money_inter_90 as {money_inter_90_name},
    t.order_coupon_num_inter_90 as {order_coupon_num_inter_90_name},
    t.coupon_money_inter_90 as {coupon_money_inter_90_name},
    
    t.u_price_avg_inter_180 as {u_price_avg_inter_180_name},
    t.t_price_avg_inter_180 as {t_price_avg_inter_180_name},
    t.order_num_inter_180 as {order_num_inter_180_name},
    t.money_inter_180 as {money_inter_180_name},
    t.order_coupon_num_inter_180 as {order_coupon_num_inter_180_name},
    t.coupon_money_inter_180 as {coupon_money_inter_180_name}
from (
    select 
        a2.userid as uid,
        
        min(case when a1.is_inter = 1 then days else null end) as order_bp_inter,
        sum(case when a1.is_inter = 1 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 then 1 else null end) as u_price_avg_inter,
        sum(case when a1.is_inter = 1 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 then a3.ticket_num else null end) as t_price_avg_inter,
        sum(case when a1.is_inter = 1 then 1 else null end) as order_num_inter,
        sum(case when a1.is_inter = 1 then a1.totalprice else null end) as money_inter,
        sum(case when a1.is_inter = 1 then a2.coupon_num else null end) as order_coupon_num_inter,
        sum(case when a1.is_inter = 1 then a2.coupon_money else null end) as coupon_money_inter,
        
        sum(case when a1.is_inter = 1 and a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 and a1.days <= 90 then 1 else null end) as u_price_avg_inter_90,
        sum(case when a1.is_inter = 1 and a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 and a1.days <= 90 then a3.ticket_num else null end) as t_price_avg_inter_90,
        sum(case when a1.is_inter = 1 and a1.days <= 90 then 1 else null end) as order_num_inter_90,
        sum(case when a1.is_inter = 1 and a1.days <= 90 then a1.totalprice else null end) as money_inter_90,
        sum(case when a1.is_inter = 1 and a1.days <= 90 then a2.coupon_num else null end) as order_coupon_num_inter_90,
        sum(case when a1.is_inter = 1 and a1.days <= 90 then a2.coupon_money else null end) as coupon_money_inter_90,
        
        sum(case when a1.is_inter = 1 and a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 and a1.days <= 180 then 1 else null end) as u_price_avg_inter_180,
        sum(case when a1.is_inter = 1 and a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.is_inter = 1 and a1.days <= 180 then a3.ticket_num else null end) as t_price_avg_inter_180,
        sum(case when a1.is_inter = 1 and a1.days <= 180 then 1 else null end) as order_num_inter_180,
        sum(case when a1.is_inter = 1 and a1.days <= 180 then a1.totalprice else null end) as money_inter_180,
        sum(case when a1.is_inter = 1 and a1.days <= 180 then a2.coupon_num else null end) as order_coupon_num_inter_180,
        sum(case when a1.is_inter = 1 and a1.days <= 180 then a2.coupon_money else null end) as coupon_money_inter_180
    from (
        select 
            *,
            case when lower(suppliercode) = 'biqu' then 0 else 1 end as is_inter,
            datediff('{date}', from_unixtime(cast(substr(orderdatetime, 0, 10) as bigint), 'yyyy-MM-dd')) as days
        from {db_src}.flights_order
    ) a1 join (
        -- 只取成功的订单, 顺便把uid也取出来, 再顺便把优惠券信息取出来
        select 
            userid, 
            orderid,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then 1 else null end) as coupon_num,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then coupondiscount else null end) as coupon_money
        from {db_src}.flights_magic_order
        where orderstatus = "SUCCESS"
        group by userid, orderid
    ) a2 on (a1.orderid = a2.orderid) join (
        -- 每个订单的票数
        select 
            orderid, 
            count(distinct orderitemid) as ticket_num
        from {db_src}.flights_order_item
        where orderid is not null and orderid <> 'null'
        group by orderid
    ) a3 on (a1.orderid = a3.orderid)
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


class IGola_UserValue3Sql(HiveTagSql):

    _table_dst = "user_value3"
    _table_dict = "user_value3_dict"
    _prefix = "value"
    _fields = {
        "order_bp_domestic":{"name":"%s_order_bp_domestic" % _prefix, "attribute":1, "type":"int", "segment":[30,90,180,360]},

        "u_price_avg_domestic":{"name":"%s_u_price_avg_domestic" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "t_price_avg_domestic":{"name":"%s_t_price_avg_domestic" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.5,0.7,0.9]},
        "order_num_domestic":{"name":"%s_order_num_domestic" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_domestic":{"name":"%s_money_domestic" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.7,0.9]},
        "order_coupon_num_domestic":{"name":"%s_order_coupon_num_domestic" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_domestic":{"name":"%s_coupon_money_domestic" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_domestic_90":{"name":"%s_u_price_avg_domestic_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_domestic_90":{"name":"%s_t_price_avg_domestic_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.4,0.6,0.9]},
        "order_num_domestic_90":{"name":"%s_order_num_domestic_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_domestic_90":{"name":"%s_money_domestic_90" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_coupon_num_domestic_90":{"name":"%s_order_coupon_num_domestic_90" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_domestic_90":{"name":"%s_coupon_money_domestic_90" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},

        "u_price_avg_domestic_180":{"name":"%s_u_price_avg_domestic_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.3,0.6,0.8,0.9]},
        "t_price_avg_domestic_180":{"name":"%s_t_price_avg_domestic_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.7,0.9]},
        "order_num_domestic_180":{"name":"%s_order_num_domestic_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "money_domestic_180":{"name":"%s_money_domestic_180" % _prefix, "attribute":1, "type":"int", "percentile":[0.2,0.5,0.8,0.9]},
        "order_coupon_num_domestic_180":{"name":"%s_order_coupon_num_domestic_180" % _prefix, "attribute":1, "type":"int", "segment":[1,2,3]},
        "coupon_money_domestic_180":{"name":"%s_coupon_money_domestic_180" % _prefix, "attribute":1, "type":"int", "segment":[50,100,200,300]},
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

    t.order_bp_domestic as {order_bp_domestic_name},
    t.u_price_avg_domestic as {u_price_avg_domestic_name},
    t.t_price_avg_domestic as {t_price_avg_domestic_name},
    t.order_num_domestic as {order_num_domestic_name},
    t.money_domestic as {money_domestic_name},
    t.order_coupon_num_domestic as {order_coupon_num_domestic_name},
    t.coupon_money_domestic as {coupon_money_domestic_name},
    
    t.u_price_avg_domestic_90 as {u_price_avg_domestic_90_name},
    t.t_price_avg_domestic_90 as {t_price_avg_domestic_90_name},
    t.order_num_domestic_90 as {order_num_domestic_90_name},
    t.money_domestic_90 as {money_domestic_90_name},
    t.order_coupon_num_domestic_90 as {order_coupon_num_domestic_90_name},
    t.coupon_money_domestic_90 as {coupon_money_domestic_90_name},
    
    t.u_price_avg_domestic_180 as {u_price_avg_domestic_180_name},
    t.t_price_avg_domestic_180 as {t_price_avg_domestic_180_name},
    t.order_num_domestic_180 as {order_num_domestic_180_name},
    t.money_domestic_180 as {money_domestic_180_name},
    t.order_coupon_num_domestic_180 as {order_coupon_num_domestic_180_name},
    t.coupon_money_domestic_180 as {coupon_money_domestic_180_name}
    
from (
    select 
        a2.userid as uid,
        
        min(case when a1.is_inter = 0 then days else null end) as order_bp_domestic,
        sum(case when a1.is_inter = 0 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 then 1 else null end) as u_price_avg_domestic,
        sum(case when a1.is_inter = 0 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 then a3.ticket_num else null end) as t_price_avg_domestic,
        sum(case when a1.is_inter = 0 then 1 else null end) as order_num_domestic,
        sum(case when a1.is_inter = 0 then a1.totalprice else null end) as money_domestic,
        sum(case when a1.is_inter = 0 then a2.coupon_num else null end) as order_coupon_num_domestic,
        sum(case when a1.is_inter = 0 then a2.coupon_money else null end) as coupon_money_domestic,
        
        sum(case when a1.is_inter = 0 and a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 and a1.days <= 90 then 1 else null end) as u_price_avg_domestic_90,
        sum(case when a1.is_inter = 0 and a1.days <= 90 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 and a1.days <= 90 then a3.ticket_num else null end) as t_price_avg_domestic_90,
        sum(case when a1.is_inter = 0 and a1.days <= 90 then 1 else null end) as order_num_domestic_90,
        sum(case when a1.is_inter = 0 and a1.days <= 90 then a1.totalprice else null end) as money_domestic_90,
        sum(case when a1.is_inter = 0 and a1.days <= 90 then a2.coupon_num else null end) as order_coupon_num_domestic_90,
        sum(case when a1.is_inter = 0 and a1.days <= 90 then a2.coupon_money else null end) as coupon_money_domestic_90,
        
        sum(case when a1.is_inter = 0 and a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 and a1.days <= 180 then 1 else null end) as u_price_avg_domestic_180,
        sum(case when a1.is_inter = 0 and a1.days <= 180 then a1.totalprice else null end) / sum(case when a1.is_inter = 0 and a1.days <= 180 then a3.ticket_num else null end) as t_price_avg_domestic_180,
        sum(case when a1.is_inter = 0 and a1.days <= 180 then 1 else null end) as order_num_domestic_180,
        sum(case when a1.is_inter = 0 and a1.days <= 180 then a1.totalprice else null end) as money_domestic_180,
        sum(case when a1.is_inter = 0 and a1.days <= 180 then a2.coupon_num else null end) as order_coupon_num_domestic_180,
        sum(case when a1.is_inter = 0 and a1.days <= 180 then a2.coupon_money else null end) as coupon_money_domestic_180
        
    from (
        select 
            *,
            case when lower(suppliercode) = 'biqu' then 0 else 1 end as is_inter,
            datediff('{date}', from_unixtime(cast(substr(orderdatetime, 0, 10) as bigint), 'yyyy-MM-dd')) as days
        from {db_src}.flights_order
    ) a1 join (
        -- 只取成功的订单, 顺便把uid也取出来, 再顺便把优惠券信息取出来
        select 
            userid, 
            orderid,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then 1 else null end) as coupon_num,
            sum(case when couponid is not null and lower(couponid) <> 'null' and cast(coupondiscount as double) > 0 then coupondiscount else null end) as coupon_money
        from {db_src}.flights_magic_order
        where orderstatus = "SUCCESS"
        group by userid, orderid
    ) a2 on (a1.orderid = a2.orderid) join (
        -- 每个订单的票数
        select 
            orderid, 
            count(distinct orderitemid) as ticket_num
        from {db_src}.flights_order_item
        where orderid is not null and orderid <> 'null'
        group by orderid
    ) a3 on (a1.orderid = a3.orderid)
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


class IGola_UserValue4Sql(HiveTagSql):
    _table_dst = "user_value4"
    _table_dict = "user_value4_dict"
    _prefix = "value"
    _fields = {
        "fareclass":{"name":"%s_fareclass" % _prefix, "attribute":2, "type":"string"},
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
    t.fareclass as {fareclass_name}
from (
    select 
        b.userid as uid,
        b.fareclass as fareclass,
        row_number() over (partition by b.userid order by b.fareclass_cnt desc) as fareclass_rank
    from (
        select 
            b2.userid as userid,
            b1.fareclass as fareclass,
            count(1) as fareclass_cnt
        from (
            select orderid, fareclass from {db_src}.flights_order
        ) b1 join (
            -- 只取成功的订单
            select userid, orderid
            from {db_src}.flights_magic_order
            where orderstatus = "SUCCESS"
            group by userid, orderid
        ) b2 on (b1.orderid = b2.orderid)
        group by b2.userid, b1.fareclass
    ) b 
) t 
where t.fareclass_rank = 1
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
