# coding: utf-8

from sql.hive_tag_collector import HiveTagCollectorSql
from .user_behavior import IGola_UserBehaviorSql
from .user_profile import IGola_UserProfileSql
from .user_value import IGola_UserValueSql
from .user_value import IGola_UserValue2Sql
from .user_value import IGola_UserValue3Sql
from .user_value import IGola_UserValue4Sql


class IGola_UserTagSql(HiveTagCollectorSql):

    _tags = [
        IGola_UserProfileSql,
        IGola_UserValueSql,
        IGola_UserValue2Sql,
        IGola_UserValue3Sql,
        IGola_UserValue4Sql,
        IGola_UserBehaviorSql
    ]
    _fields = [field for tag in _tags for field in tag._fields.itervalues()]
