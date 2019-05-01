# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_weather
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_weather
-- 源表: peacebird.weather_view
-- 目标表: edw.dim_weather
-- 程序描述: 天气信息表
-- 程序路径: /opt/peacebird/edw/p_dim_weather.sql
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
spark_conf.set("spark.sql.shuffle.partitions", '5')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
        insert overwrite table edw.dim_weather
        select
            id as id
            , cid as city_id
            , location as city
            , parent_city as parent_city
            , admin_area as admin_area
            , cnty as country
            , lat as lat
            , lon as lon
            , tz as time_zone
            , cast(from_unixtime(unix_timestamp(loc,'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd HH:mm:dd') as timestamp) as time_local
            , cast(from_unixtime(unix_timestamp(utc,'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd HH:mm:dd') as timestamp) as time_utc
            , cond_code_d as weather_code_day
            , cond_code_n as weather_code_night
            , cond_txt_d as weather_txt_day
            , cond_txt_n as weather_txt_night
            --, "date" as weather_date
            , `date` as weather_date
            , hum as humidity
            , pcpn as precipitation
            , pop as precipitation_probability
            , pres as pressure
            , tmp_max as temperature_max
            , tmp_min as temperature_min
            , uv_index as uv_rays
            , vis as visibility
            , wind_deg as wind_deg
            , wind_dir as wind_dir
            , wind_sc as wind_sc
            , wind_spd as wind_spd
            , createtime as createtime
            , current_timestamp as etl_time
        from peacebird.weather_view
"""

spark.sql(sql)
