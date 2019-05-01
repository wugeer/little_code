# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_skc_store_day_sales
   Description :
   Author :       zsm
   date：          2018/8/6
-------------------------------------------------
   Change Activity:
                   2018/8/6:
                   2018/12/26: 排除加盟的门店
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_skc_store_day_sales.py
-- 源表: edw.fct_sales
--       edw.dim_store
         edw.dim_product_skc
         edw.dim_product
-- 目标表: edw.mid_skc_store_day_sales
-- 程序描述: 出入库事实表
-- 程序路径: /opt/peacebird/edw/p_mid_skc_store_day_sales.sql
-- 程序备注:
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""
import time
import sys
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import Row

import os

time_start=time.time()

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

p_input_date = "'"+sys.argv[1]+"'"

#spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

sql = """
        insert overwrite table edw.mid_skc_store_day_sales partition(day_date)
        select 
            fs.product_id
            , dps.product_code
            , fs.color_id
            , dps.color_code
            , fs.store_id
            , ds.store_code
            , sum(fs.qty) as sales_qty
            , sum(fs.real_amt) as sales_amt
            , null as total_sales_qty
            , null as total_sales_amt
            , current_timestamp as etl_time
            , {p_input_date} as day_date
        from edw.fct_sales as fs 
        inner join edw.dim_target_store ds    -- 排除 加盟门店 的销售数据
            on fs.store_id = ds.store_id
        inner join edw.dim_product_skc as dps 
            on fs.product_id = dps.product_id and fs.color_id = dps.color_id
        where fs.sale_date = {p_input_date}
        group by fs.product_id,dps.product_code, fs.color_id,dps.color_code, fs.store_id,ds.store_code
""".format(**{"p_input_date": p_input_date})


spark.sql(sql)

# time_end=time.time()
# print('time cost:{0}'.format(time_end-time_start))

