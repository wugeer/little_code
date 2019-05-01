# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_io
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_fct_io
-- 源表: peacebird.fa_storage_ftp_view
--       peacebird.m_attributesetinstance_view
         edw.dim_product_sku
         edw.dim_store
-- 目标表: edw.fct_io
-- 程序描述: 出入库事实表
-- 程序路径: /opt/peacebird/edw/p_fct_io.sql
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
spark_conf.set("spark.sql.shuffle.partitions", '20')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
        insert overwrite table edw.fct_io
        select fsf.id as doc_id
            , fsf.creationdate as io_time
            , date_format(fsf.creationdate, 'yyyy-MM-dd') as io_date
            , fsf.c_store_id as org_id
            , fsf.m_product_id as product_id
            , ma.value1_id as color_id
            , ma.value2_id as size_id
            , fsf.billtypename as io_type
            , fsf.qty*fsf.state as qty
            , fsf.qtyamount*fsf.state as amt
            , current_timestamp as etl_time
        from peacebird.fa_storage_ftp_view as fsf
        left join peacebird.m_attributesetinstance_view as ma on fsf.m_attributesetinstance_id = ma.id
        inner join edw.dim_product_sku as dps on fsf.m_product_id = dps.product_id
            and ma.value1_id = dps.color_id and ma.value2_id = dps.size_id
        inner join edw.dim_store as ds on fsf.c_store_id = ds.store_id
        where fsf.isactive='Y'
"""

spark.sql(sql)
