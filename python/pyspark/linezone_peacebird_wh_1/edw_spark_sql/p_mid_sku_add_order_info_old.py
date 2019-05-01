# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_sku_add_order_info_old
   Description :
   Author :       liuhai
   date：          2018/10/25
-------------------------------------------------
   Change Activity:
                   2018/10/25:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_sku_add_order_info
-- 源表: peacebird.chasingdata_view
         edw.dim_product_sku
-- 目标表:  edw.mid_sku_add_order_info_old
-- 程序描述: 追单信息表
-- 程序路径: /opt/peacebird/edw/p_mid_sku_add_order_info_old.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

# 追单历史信息 首先 存一个月的数据 然后观察数据的 同一个skc的追单号 是否相同， 
# 如果相同 可以用 追单号 + 日期 进行group by 然后取 追单号 +  最新日期  筛选掉 该追单号 其他日期的 记录

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

p_input_date = "'" + sys.argv[1] + "'"

sql = """
    insert overwrite table edw.mid_sku_add_order_info_old
    select 
        a.docno as order_id
        , from_unixtime(unix_timestamp(a.zdbilldate,'yyyyMMdd'),'yyyy-MM-dd') as order_date       
        , b.sku_id
        , b.product_id
        , b.color_id
        , b.size_id
        , a.zdqty as add_order_qty 
        , a.dhqty as arrived_qty
        , (a.zdqty - a.dhqty) as not_arrived_qty
        , {p_input_date} as day_date                      -- 备份日期
        , current_timestamp as etl_time 
    from peacebird.chasingdata_view a 
    inner join edw.dim_product_sku b on a.m_productalias_id = b.sku_id
    
    union all 
    
    select * 
    FROM edw.mid_sku_add_order_info_old
    where day_date <> {p_input_date} 
        
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)


