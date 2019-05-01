# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_webposdisstoreitem
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_webposdisstoreitem
-- 源表: peacebird.c_webposdisstoreitem
-- 目标表: edw.dim_webposdisstoreitem
-- 程序描述: 门店活动维度表
-- 程序路径: /opt/peacebird/edw/p_dim_webposdisstoreitem.sql
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
        insert overwrite table edw.dim_webposdisstoreitem
        select
            cwpdsi.id as id
            , cwpdsi.c_webposdis_id as act_id
            , cwpdsi.c_store_id as store_id
            , cwpdsi.creationdate as creation_date
            , cwpdsi.modifieddate as modified_date
            , cwpdsi.isactive as act_store_is_active
            , current_timestamp as etl_time
        from peacebird.c_webposdisstoreitem_view as cwpdsi
        inner join edw.dim_store as ds on cwpdsi.c_store_id = ds.store_id
"""

spark.sql(sql)

