# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_skc_day_road_stock.py
   Description :
   Author :       zsm
   date：          2018/8/14
-------------------------------------------------
   Change Activity:
                   2018/8/14:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_skc_day_road_stock.py
-- 源表: 
-- 目标表:mid_skc_day_road_stock
-- 程序描述: skc全国在途库存
-- 程序路径: /opt/peacebird/edw_spark_sql/p_mid_skc_day_road_stock.py
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import sys

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


sql="""
insert overwrite table edw.mid_skc_day_road_stock
partition(day_date)
select
    a.product_id,
    a.color_id,
    sum(send_qty) as road_stock_qty,
    current_timestamp as etl_time,
    {p_input_date} as day_date
from edw.fct_stock a
where (a.send_date > date_sub({p_input_date}, 14) and a.send_date <= {p_input_date})
    and (a.receive_date > {p_input_date} or a.receive_date is null)
group by product_id, color_id

--union all

--select * from edw.mid_skc_day_road_stock
--where day_date <> {p_input_date}
""".format(**{"p_input_date":p_input_date})
spark.sql(sql)
