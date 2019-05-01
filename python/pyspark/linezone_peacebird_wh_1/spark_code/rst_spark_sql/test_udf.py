# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     test_udf
   Description :
   Author :       yangming
   date：          2018/9/13
-------------------------------------------------
   Change Activity:
                   2018/9/13:
-------------------------------------------------
"""
import os
import sys
from datetime import timedelta
import datetime


def date_trunc(interval, date_str):
    """
    截断到指定精度，返回相应的日期字符串
    :param interval: ['week', 'month', 'year']
    :param date_str: 
    :return: after_trunc_date_str
    """
    # TODO 以后兼容各种日期格式，现在只支持 2018-09-12 标准日期字符串
    # date_obj = parse(date_str)
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == 'week':
        res = date_obj - timedelta(days=(date_obj.isocalendar()[2] - 1))
    elif interval == 'month':
        res = datetime.date(date_obj.year, date_obj.month, 1)
    elif interval == 'year':
        res = datetime.date(date_obj.year, 1, 1)
    else:
        raise Exception("interval must be ['week', 'month', 'year']")
    return res.strftime('%Y-%m-%d')

from utils.tools import SparkInit
from utils.config import PBConfig


if __name__ == '__main__':
    # file_name = os.path.basename(__file__)
    foo = SparkInit('123')
    print(PBConfig.SELL_WELL_RATE)
    # foo.spark.sql("select product_id from edw.dim_product").show()
    foo.spark.udf.register('udf_date_trunc', date_trunc)
    foo.spark.sql("select udf_date_trunc('week', '2018-09-13')").show()

