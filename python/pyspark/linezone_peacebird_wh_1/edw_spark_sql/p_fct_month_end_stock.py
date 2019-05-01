# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_month_end_stock
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_fct_month_end_stock
-- 源表: peacebird.fa_monthstore_view
--       peacebird.c_period_view
--       peacebird.m_attributesetinstance_view
--       edw.dim_product_sku
--       edw.dim_store
-- 目标表: edw.fct_month_end_stock
-- 程序描述: 月结库存表
-- 程序路径: /opt/peacebird/edw/p_fct_month_end_stock.sql
-- 程序备注:
-- 版本号信息: v1.0 create
--             v1.1 修改关联到产品sku表和门店仓库维表
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
spark_conf.set("spark.sql.shuffle.partitions", '10')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
        insert overwrite table edw.fct_month_end_stock
        select
            from_unixtime(unix_timestamp(cp.dateend,'yyyyMMdd'),'yyyy-MM-dd') as stock_date
            , fm.c_store_id as org_id
            , fm.m_product_id as product_id
            , ma.value1_id as color_id
            , ma.value2_id as size_id
            , fm.qtyend as stock_qty
            , fm.percostend as stock_price
            , fm.costend as stock_amt
            , substring(cp.yearmonth, 0, 4) as year
            , substring(cp.yearmonth, 5, 6) as month
            , current_timestamp() as etl_time
        from peacebird.fa_monthstore_view as fm
        left join peacebird.c_period_view as cp on fm.c_period_id = cp.id
        left join peacebird.m_attributesetinstance_view as ma on fm.m_attributesetinstance_id = ma.id
        inner join edw.dim_product_sku as dps on fm.m_product_id = dps.product_id
            and ma.value1_id = dps.color_id and ma.value2_id = dps.size_id
        inner join edw.dim_store as ds on fm.c_store_id = ds.store_id
"""

spark.sql(sql)
