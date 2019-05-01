# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_weather_his
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_weather_his
-- 源表: peacebird.wetherhisday_view
-- 目标表: edw.dim_weather_his
-- 程序描述: 天气历史信息表
-- 程序路径: /opt/peacebird/edw/p_dim_weather_his.sql
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
        insert overwrite table edw.dim_weather_his
        select id as id
            , rowid as rowid
            , city as city
            , cnty as country
            , parent_city as parent_city
            , prov as province
            , lat as lat
            , lon as lon
            , brief as weather
            , code as weather_code
            , date as weather_date
            , hightmp as temperature_max
            , lowtmp as temperature_min
            , current_timestamp as etl_time
        from peacebird.wetherhisday_view
"""

spark.sql(sql)
