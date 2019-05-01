# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_webposdisproductitem
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_webposdisproductitem
-- 源表: peacebird.c_webposdisproductitem
        edw.dim_product
-- 目标表: edw.dim_webposdisproductitem
-- 程序描述: 产品活动维度表
-- 程序路径: /opt/peacebird/edw/p_  .sql
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
        insert overwrite table edw.dim_webposdisproductitem
        select cwpdpi.id as id
            , cwpdpi.c_webposdis_id as act_id
            , cwpdpi.m_product_id as product_id
            , cwpdpi.creationdate as creation_date
            , cwpdpi.modifieddate as modified_date
            , cwpdpi.isactive as is_active
            , current_timestamp as etl_time
        from  peacebird.c_webposdisproductitem_view as cwpdpi
        inner join edw.dim_product as dp on cwpdpi.id = dp.product_id
"""

spark.sql(sql)

