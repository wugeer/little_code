# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_day_end_stock_peacebird.py
   Description :
   Author :       zsm
   date：          2018/8/17
-------------------------------------------------
   Change Activity:
                   2018/8/17:
                   2018/11/09:CB37 的日末库存取追单可配量
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

p_input_date = "'"+sys.argv[1]+"'"

sql = """
    insert overwrite table edw.mid_day_end_stock_peacebird
    SELECT
    {p_input_date} as stock_date
    , store.store_id as org_id
    , store.store_code as org_code
    , store.store_type as org_type
    , sku.product_id
    , sku.product_code
    , sku.color_id
    , sku.color_code
    , sku.size_id
    , sku.size_code
    --, cast(stock.qty as int) as stock_qty
    , (case
            when stock.c_store_id <> '420867' then cast(stock.qty as int)  -- 当门店 编码不等于 CB37 取当前库存作为日末库存
            else  cast(stock.qtyconsign as int)                            -- 当门店 编码 等于  CB37 取追单可配 作为仓库CB37的日末库存
        end
        ) as stock_qty
    , cast(amt_priceqty as decimal(30,8)) as stock_amt
    , CURRENT_TIMESTAMP as etl_time
    from peacebird.fa_storage_view as stock
    inner join edw.dim_product_sku as sku
        on stock.m_productalias_id=sku.sku_id
    inner join edw.dim_store as store
        on stock.c_store_id=store.store_id
    union ALL
    select * from edw.mid_day_end_stock_peacebird
    where stock_date<>{p_input_date}
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)



