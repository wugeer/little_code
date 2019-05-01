# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_sku_add_order_info
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_sku_add_order_expected_arrival
-- 源表: peacebird.chasingdata_view
         edw.dim_product_sku
-- 目标表:  edw.mid_sku_add_order_info
-- 程序描述: 追单信息表
-- 程序路径: /opt/peacebird/edw/p_mid_sku_add_order_info.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf

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
        insert overwrite table edw.mid_sku_add_order_info
        select a.docno as order_id
            , from_unixtime(unix_timestamp(a.zdbilldate,'yyyyMMdd'),'yyyy-MM-dd') as order_date       
            , b.sku_id
            , b.product_id
            , b.color_id
            , b.size_id
            , a.zdqty as add_order_qty 
            , a.dhqty as arrived_qty
            , (a.zdqty - a.dhqty) as not_arrived_qty
            , current_timestamp as etl_time 
        from peacebird.chasingdata_view a 
        inner join edw.dim_product_sku b on a.m_productalias_id = b.sku_id
"""

spark.sql(sql)
