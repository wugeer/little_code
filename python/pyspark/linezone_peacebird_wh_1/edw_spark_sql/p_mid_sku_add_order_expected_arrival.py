# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_sku_add_order_expected_arrival
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
--       peacebird.chasingdataitem_view
         edw.dim_product_sku
-- 目标表:  edw.mid_sku_add_order_expected_arrival
-- 程序描述: 追单预计到货表
-- 程序路径: /opt/peacebird/edw/p_mid_sku_add_order_expected_arrival.sql
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
        insert overwrite table edw.mid_sku_add_order_expected_arrival
        select b.docno as order_id
            , c.sku_id 
            , c.product_id
            , c.color_id
            , c.size_id 
            , from_unixtime(unix_timestamp(a.dateyj,'yyyyMMdd'),'yyyy-MM-dd') as expected_arrive_date
            , a.qtyyj as expected_arrive_qty
            , from_unixtime(unix_timestamp(a.datesj,'yyyyMMdd'),'yyyy-MM-dd') as actual_arrive_date
            , a.qtysj as actual_arrive_qty
            , current_timestamp as etl_time
        from peacebird.chasingdataitem_view a 
        inner join peacebird.chasingdata_view b on a.chasingdata_id = b.id
        inner join edw.dim_product_sku c on b.m_productalias_id = c.sku_id
"""

spark.sql(sql)
