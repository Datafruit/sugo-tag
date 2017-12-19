# coding: utf-8

import json
from random import uniform
from random import gauss
from zlib import crc32
import pandas as pd

if __name__ == "__main__":
    # tag_conf = [
    #     {"name": "gender", "type": 2, "tags": ["male", "female", None]},
    #     {"name": "age", "type": 1, "tags": [0, 18, 22, 30, 40, None]},
    #     {"name": "platform", "type": 2, "tags": ["IOS", "Android", "PC", None]},
    #     {"name": "reg_bp", "type": 1, "tags": [0, 30, 60, 90, 180, None]},
    #     {"name": "login_bp", "type": 1, "tags": [0, 7, 14, 30, 60, None]},
    #     {"name": "channel", "type": 2, "tags": ["c1", "c2", "c3", "c4", "c5", None]},
    #     {"name": "order_num", "type": 1, "tags": [0, 1, 2, 3, 4, None]},
    #     {"name": "money_total", "type": 1, "tags": [0, 100, 200, 300, 400, 500, None]},
    #     {"name": "price_avg", "type": 1, "tags": [0, 50, 100, 150, None]},
    # ]

    tag_conf = [
        {"name": "profile_gender", "type": 2, "tags": ["male", "female", None]},
        {"name": "profile_age", "type": 1, "tags": [0, 25, 30, 35, 40, 50, None]},
        {"name": "profile_reg_bp", "type": 1, "tags": [0, 30, 60, 90, 180, 365, None]},
        {"name": "profile_login_bp", "type": 1, "tags": [0, 30, 60, 90, 180, 365, None]},
        {"name": "profile_payment_method", "type": 2, "tags": ["微信支付", "支付宝支付", "银行卡支付", None]},
        {"name": "profile_user_login_num_90", "type": 1, "tags": [0, 1, 3, 5, 10, None]},
        {"name": "profile_user_login_num_180", "type": 1, "tags": [0, 3, 5, 8, 12, None]},
        {"name": "profile_user_view_num_90", "type": 1, "tags": [0, 5, 15, 20, 25, None]},
        {"name": "profile_user_view_num_180", "type": 1, "tags": [0, 5, 15, 20, 25, 30, None]},
        {"name": "profile_user_click_num_90", "type": 1, "tags": [0, 3, 5, 8, 12, 15, 20, None]},
        {"name": "profile_user_click_num_180", "type": 1, "tags": [0, 3, 5, 8, 12, 15, 20, None]},
        {"name": "profile_edu", "type": 2, "tags": ["初中", "高中", "大专", "本科", "硕士", "博士", None]},
        {"name": "profile_marriage", "type": 2, "tags": ["已婚", "未婚", None]},
        {"name": "profile_child", "type": 2, "tags": ["是", "否", None]},
        {"name": "profile_loan", "type": 2, "tags": ["是", "否", None]},
        {"name": "profile_house_num", "type": 1, "tags": [0, 1, 2, 3, 4, None]},
        {"name": "profile_investment", "type": 2, "tags": ["是", "否", None]},
        {"name": "profile_business", "type": 2, "tags": ["服务商会", "餐饮商会", "轻工商会", "纺织商会", None]},
        {"name": "profile_occupation", "type": 2, "tags": ["技术人员", "小企业主", "行政管理人员", "厨师", None]},
        {"name": "profile_industry", "type": 2, "tags": ["餐旅业", "交通运输业", "农牧业", "金融业", "互联网", "服务业", "广告业", None]},
        {"name": "profile_consumption", "type": 1, "tags": [0, 10000, 50000, 100000, 300000, 500000, None]},
        {"name": "profile_consumption_type", "type": 2, "tags": ["房产汽车类", "有形商品类", "贷款租赁类", "服务享受类", "生活缴费类", "其他", None]},
        {"name": "profile_risk", "type": 2, "tags": ["保守型", "收益型", "积极型", "稳健型", "激进型", "进取型", None]},
        {"name": "profile_type", "type": 2, "tags": ["中小户型", "精装房", "大户型", "复式楼", "商铺", "公寓", "别墅", None]},
        {"name": "profile_char", "type": 2, "tags": ["临地铁", "精装修", "现房", "自由购", "品牌地产",  None]},
        {"name": "profile_layout", "type": 2, "tags": ["一居", "二居", "三居", "四居", "五居及以上", None]},
        {"name": "profile_order_num", "type": 1, "tags": [0, 1, 2, 3, 4, 5, 10, None]},
        {"name": "profile_trade", "type": 1, "tags": [0, 10000, 50000, 100000, 300000, 500000, 1000000, None]},
        {"name": "profile_complaint", "type": 2, "tags": ["投诉", "建议", None]},
        {"name": "profile_area", "type": 1, "tags": [0, 50, 80, 120, 140, 160, None]},
        {"name": "profile_consumption_avg", "type": 1, "tags": [0, 10000, 50000, 100000, 300000, None]},
        {"name": "profile_periphery", "type": 2, "tags": ["生活必需设施", "医疗设施", "教育设施", "文化体育设施", None]},
        {"name": "profile_user_share_num_90", "type": 1, "tags": [0, 1, 2, 3, 5, None]},
        {"name": "profile_user_share_num_180", "type": 1, "tags": [0, 1, 2, 3, 5, None]},
        {"name": "profile_user_comment_num_90", "type": 1, "tags": [0, 1, 2, 3, 5, None]},
        {"name": "profile_user_comment_num_180", "type": 1, "tags": [0, 1, 2, 3, 5, None]},
        {"name": "profile_floor", "type": 1, "tags": [0, 6, 10, 13, 18, 22, 30, None]},
        {"name": "profile_income", "type": 1, "tags": [0, 10000, 50000, 100000, 300000, 500000, None]},
        {"name": "profile_car", "type": 2, "tags": ["是", "否", None]},
        {"name": "profile_house", "type": 2, "tags": ["是", "否", None]},
    ]
    tag_conf_dt = {v["name"]: {"type": v["type"], "tags": v["tags"]} for v in tag_conf}

    users = []
    for i in xrange(1, 100000):
        user_id = "%06d" % i
        seed = "%02d" % (crc32(user_id) % 10)
        for conf in tag_conf:
            origin = crc32(seed + conf["name"]) % len(conf["tags"])
            offset = int(gauss(0, 1))
            index = origin + offset % len(conf["tags"]) - len(conf["tags"])
            if conf["tags"][index] is None:
                continue
            elif conf["type"] == 2:
                tag_value = conf["tags"][index]
                tag_name = tag_value
                value = tag_value
            elif conf["type"] == 1:
                lower = conf["tags"][index]
                if conf["tags"][index+1] is not None:
                    upper = conf["tags"][index+1] if conf["tags"][index+1] is not None else conf["tags"][index] * 2
                    tag_value = "%d`%d" % (lower, upper)
                    tag_name = "(%d, %d]" % (lower, upper)
                    value = int(uniform(upper, lower))
                else:
                    upper = conf["tags"][index] * 2
                    tag_value = "%d`" % lower
                    tag_name = ">%d" % lower
                    value = int(uniform(upper, lower))
            else:
                raise
            # tag = "%s`%s" % (conf["name"], tag_name)
            # users.append((user_id, tag, 1))
            users.append((user_id, conf["name"], tag_name, tag_value, value))

    df = pd.DataFrame(data=users, columns=["user_id", "name", "tag_name", "tag_value", "value"])

    ## user_tag
    df_user_tag = df[["user_id", "name", "value"]]
    df_user_tag = pd.pivot_table(df_user_tag, values=["value"], index=["user_id"], columns=["name"], aggfunc=max)
    df_user_tag.reset_index(inplace=True)
    df_user_tag.columns = [c2 if c2 else c1 for c1, c2 in df_user_tag.columns.tolist()]
    df_user_tag.fillna("\N", inplace=True)
    # print df_user_tag
    with open("/Users/penghuan/Tmp/tag/dichan/user_tag_columns.txt", 'w') as fd:
        col_type_dt = {1: "int", 2: "string"}
        for col_name in df_user_tag.columns.tolist():
            col_type = tag_conf_dt[col_name]["type"] if col_name in tag_conf_dt else 2
            print >> fd, '{"name": "%s", "type": "%s"}' % (col_name, col_type_dt[col_type])
    df_user_tag.to_csv("/Users/penghuan/Tmp/tag/dichan/user_tag.txt", sep='\t', header=False, index=False)

    ## user_tag_dict
    df_user_tag_dict = df[["name", "tag_name", "tag_value"]]
    df_user_tag_dict = pd.DataFrame(df_user_tag_dict.groupby(["name", "tag_name", "tag_value"], as_index=False).size())
    df_user_tag_dict.reset_index(inplace=True)
    def get_type(row):
        return filter(lambda x: x["name"]==row["name"], tag_conf)[0]["type"]
    df_user_tag_dict["type"] = df_user_tag_dict.apply(lambda row: get_type(row), axis=1)
    df_user_tag_dict = df_user_tag_dict[["name", "type", "tag_name", "tag_value"]]
    # print df_user_tag_dict
    df_user_tag_dict.to_csv("/Users/penghuan/Tmp/tag/dichan/user_tag_dict.txt", sep='\t', header=False, index=False)

    ## user_tag_tag
    df_user_tag_tag = df[["user_id", "name", "tag_name"]]
    df_user_tag_tag["tag"] = df_user_tag_tag.apply(lambda row: row["name"]+"`"+row["tag_name"], axis=1)
    df_user_tag_tag["value"] = 1
    df_user_tag_tag = df_user_tag_tag[["user_id", "tag", "value"]]
    # print df_user_tag_tag
    df_user_tag_tag.to_csv("/Users/penghuan/Tmp/tag/dichan/user_tag_tag.txt", sep='\t', header=False, index=False)


    # df = pd.DataFrame(
    #     data=users,
    #     columns=["user_id", "tag", "value"],
    # )
    # df = pd.pivot_table(df, values=["value"], index=["user_id"], columns=["tag"])
    # df.reset_index(inplace=True)
    # df.columns = [c2 if c2 else c1 for c1, c2 in df.columns.tolist()]
    #
    # print df.shape[0]
    # print df.groupby("platform`IOS").size()
    # print df.groupby("platform`Android").size()
    # print df.groupby("platform`PC").size()
    # print "###########"
    # print df[df["gender`male"] == 1].shape[0]
    # print df[df["gender`male"] == 1].groupby("platform`IOS").size()
    # print df[df["gender`male"] == 1].groupby("platform`Android").size()
    # print df[df["gender`male"] == 1].groupby("platform`PC").size()
    # print "###########"
    # print df[df["gender`female"] == 1].shape[0]
    # print df[df["gender`female"] == 1].groupby("platform`IOS").size()
    # print df[df["gender`female"] == 1].groupby("platform`Android").size()
    # print df[df["gender`female"] == 1].groupby("platform`PC").size()

