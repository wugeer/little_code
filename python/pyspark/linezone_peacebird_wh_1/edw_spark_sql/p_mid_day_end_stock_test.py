# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_day_end_stock_test.py
   Description :
   Author :       zsm
   date：          2018/8/17
-------------------------------------------------
   Change Activity:
                   2018/8/17:
-------------------------------------------------
"""

from os.path import abspath
from pyspark.sql import SparkSession
import os
import sys


warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')

# app_name
file_name = os.path.basename(__file__)
app_name = "".join(["PySpark-", file_name])

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

p_input_date = "'" + sys.argv[1] + "'"

sql = """
    insert overwrite table edw.mid_day_end_stock_test
    SELECT
    {p_input_date} as stock_date
    , store.store_id as org_id
    , store.store_type as org_type
    , sku.product_id
    , sku.color_id
    , sku.size_id
    , cast(stock.qty as int) as stock_qty
    , 0 as receive_qty
    , 0 as send_qty
    , CURRENT_TIMESTAMP as etl_time
    from peacebird.fa_storage_view as stock
    inner join edw.dim_product_sku as sku
        on stock.m_productalias_id = sku.sku_id
    inner join edw.dim_store as store
        on stock.c_store_id = store.store_id
    inner join (select * from edw.dim_target_product where day_date={p_input_date}) as target_pro
        on sku.product_id = target_pro.product_id and store.dq_long_code = target_pro.dq_long_code
    union ALL
    select * from edw.mid_day_end_stock_test
    where stock_date <> {p_input_date}
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)



