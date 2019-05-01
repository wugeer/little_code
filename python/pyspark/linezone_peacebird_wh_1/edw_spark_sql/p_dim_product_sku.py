# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_product_sku
   Description :
   Author :       yangming
   date：          2018/8/3
-------------------------------------------------
   Change Activity:
                   2018/8/3:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_product_sku
-- 源表:  peacebird.m_product_alias_view
--        peacebird.m_attributesetinstance_view
--        edw.dim_product_skc
-- 目标表: edw.dim_product_sku
-- 程序描述: 产品sku维表
-- 程序路径: /opt/peacebird/edw/p_dim_product_sku.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
-------------------------------------------------------
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
        insert overwrite table edw.dim_product_sku
        select mpa.id as sku_id
            , mpa.no as sku_code    -- 1105 表新增字段
            , max(mpa.m_product_id) as product_id
            , max(dps.product_code) as product_code
            , max(dps.product_name) as product_name
            , max(ma.value1_id) as color_id
            , max(ma.value1_code) as color_code
            , max(ma.value1) as color_name
            , max(ma.value2_id) as size_id
            , max(ma.value2_code) as size_code
            , max(ma.value2) as size_name
            , max(ma.isactive) as status
            , null as order_num
            , current_timestamp as etl_time
        from peacebird.m_product_alias_view mpa
        inner join peacebird.m_attributesetinstance_view ma on
            mpa.m_attributesetinstance_id = ma.id
        inner join edw.dim_product_skc dps on 
            mpa.m_product_id=dps.product_id and ma.value1_id=dps.color_id
        group by mpa.id,mpa.no
"""

spark.sql(sql)

