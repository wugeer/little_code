# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_movetype
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_movetype
-- 源表:  peacebird.dim_movetype_view
-- 目标表: edw.dim_movetype
-- 程序描述: 库存移动事实表
-- 程序路径: /opt/peacebird/edw/p_dim_movetype.sql
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
        insert overwrite table edw.dim_movetype
        select id as movetype_code
            , value as movetype_name
            , isactive as is_active
            , description as remark
            , current_timestamp as etl_time
        from peacebird.dim_movetype_view
"""

spark.sql(sql)
