# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_lifecycle_otb_sales
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_lifecycle_otb_sales
-- 源表: 
-- 目标表: mid_lifecycle_otb_sales
-- 程序描述: 
-- 程序路径: /opt/peacebird/edw/p_mid_lifecycle_otb_sales.sql
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
        insert overwrite table edw.mid_lifecycle_otb_sales
        select 
            cast(m_dim2_id as int) as year_id
            , m_dim6_id as quarter_id
            , m_dim8_id as band
            , m_dim23_id as mictiny_class  --细分品类
            , t_day_week_of_year as week_date  -- 年周
            , cast(sale_percent as decimal(30, 8)) as sale_percent  -- 销售权重
            , current_timestamp as etl_time
        from peacebird.m_category_lifecycle_view
"""

spark.sql(sql)
