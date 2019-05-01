# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_day_end_stock.py
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
    insert overwrite table edw.mid_day_end_stock
    partition(stock_date)
    select 
        store.store_id as org_id
        , store.store_type as org_type
        , sku.product_id
        , sku.color_id
        , sku.size_id
        --, cast(stock.qty as int) as stock_qty
        , (case
            when stock.c_store_id <> '420867' then cast(stock.qty as int)  -- 当门店 编码不等于 CB37 取当前库存作为日末库存
            else  cast(stock.qtyconsign as int)                            -- 当门店 编码 等于  CB37 取追单可配 作为仓库CB37的日末库存
        end
        ) as stock_qty
        , 0 as receive_qty
        , 0 as send_qty
        , current_timestamp as etl_time
        , {p_input_date} as stock_date
    from peacebird.fa_storage_view as stock
    inner join edw.dim_product_sku as sku
        on stock.m_productalias_id = sku.sku_id
    inner join (select * from edw.dim_store where is_store='Y') as store
        on stock.c_store_id = store.store_id
    inner join (select * from edw.dim_target_product where day_date={p_input_date}) as target_pro
        on sku.product_id = target_pro.product_id and store.dq_long_code = target_pro.dq_long_code
    union all 
    select 
        store.store_id as org_id
        , store.store_type as org_type
        , sku.product_id
        , sku.color_id
        , sku.size_id
        --, cast(stock.qty as int) as stock_qty
        , (case
            when stock.c_store_id <> '420867' then cast(stock.qty as int)  -- 当门店 编码不等于 CB37 取当前库存作为日末库存
            else  cast(stock.qtyconsign as int)                            -- 当门店 编码 等于  CB37 取追单可配 作为仓库CB37的日末库存
        end
        ) as stock_qty
        , 0 as receive_qty
        , 0 as send_qty
        , current_timestamp as etl_time
        , {p_input_date} as stock_date
    from peacebird.fa_storage_view as stock
    inner join edw.dim_product_sku as sku
        on stock.m_productalias_id = sku.sku_id
    inner join (select * from edw.dim_store where is_store='N') as store
        on stock.c_store_id = store.store_id
    inner join (select product_id from edw.dim_target_product where day_date={p_input_date} group by product_id) as target_pro
        on sku.product_id = target_pro.product_id
    --union all
    --select * from edw.mid_day_end_stock
    --where stock_date <> {p_input_date}
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)



