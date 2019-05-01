# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_business_advice
   Description :
   Author :       liuhai
   date：          2018/8/17
-------------------------------------------------
   Change Activity:
                   2018/8/17:
-------------------------------------------------
-- 项目: peacebird
-- 过程名: p_fct_business_advice
-- 源表: peacebird.mf_shengyicanmou_view
-- 目标表: edw.fct_business_advice
-- 程序描述: 可比门店表
-- 程序路径: /opt/peacebird/edw/p_fct_business_advice.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf

import time
import sys
import os

warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')

# app_name
file_name = os.path.basename(__file__)
app_name = "".join(["PySpark-", file_name])

# config 配置
spark_conf = SparkConf()
spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
# 解决 分区下 小文件过多的问题
spark_conf.set("spark.sql.shuffle.partitions", '1')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
              --模型需要调用的数据表
      insert overwrite table edw.fct_business_advice
      SELECT 
      brandid
      , date
      , flat
      , productid as product_id
      , scan
      , visitornum
      , avg_staytime
      , rate_detailpage
      , rate_order
      , rate_orderpay
      , rate_pay
      , orderamount
      , orderqty
      , orderpeoplenum
      , payamount
      , payqty
      , addpayqty
      , avg_visitorvalue
      , hitcount
      , rate_hit
      , rate_exposure
      , collectpeoplenum
      , buyernum_searchguide
      , avg_price
      , rate_searchpay
      , visitornum_searchboot
      , buyernum
      , returnamount_sale
      , returnbillnum_sale
      , loaddate
      , current_timestamp as etl_time
      FROM peacebird.mf_shengyicanmou_view
"""

spark.sql(sql)
