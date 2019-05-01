# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_city_location
   Description :
   Author :       yangming
   date：          2018/8/3
-------------------------------------------------
   Change Activity:
                   2018/8/3:
-------------------------------------------------
-- 项目: peacebird
-- 过程名: p_dim_city_location
-- 源表: peacebird.weather_view
-- 目标表: edw.dim_city_location
-- 程序描述: 城市信息表
-- 程序路径: /opt/peacebird/edw/p_dim_city_location.sql
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
        insert overwrite table edw.dim_city_location
        select location as areaname
            , parent_city as parent_city
            , admin_area as shortname
            , null as areacode
            , null as zipcode
            , null as pinyin
            , cnty as cnty
            , lat as lat
            , lon as lng
            , null as city_level
            , null as city_position
            , null as sort
            , current_timestamp as etl_time
        from peacebird.weather_view
        group by location, parent_city, admin_area, cnty, lat, lon
"""
spark.sql(sql)
