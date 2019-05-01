# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_comp_store
   Description :
   Author :       yangming
   date：          2018/8/8
-------------------------------------------------
   Change Activity:
                   2018/8/8:
-------------------------------------------------
-- 项目: peacebird
-- 过程名: p_dim_comp_store
-- 源表: peacebird.c_store_bi_view
-- 目标表: edw.dim_comp_store
-- 程序描述: 可比门店表
-- 程序路径: /opt/peacebird/edw/p_dim_comp_store.sql
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

p_input_date = repr(sys.argv[1])

sql = """
        insert overwrite table edw.dim_comp_store
        partition(day_date)
        SELECT
            c_store_id as comp_store_id  -- 可比门店id         
            , current_timestamp as etl_time
            , {p_input_date} as day_date  -- 年周
        from peacebird.c_store_bi_view
        --union all
        --select * from edw.dim_comp_store
        --where day_date <> {p_input_date}
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)