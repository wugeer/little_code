# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_sku_org_day_road_stock_peacebird
   Description :
   Author :       yangming
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

p_input_date = repr(sys.argv[1])

#  -- 取传入日期当天去重后 的 目标产品id
sql_tmp_target_product = '''
    select 
        product_id 
    from edw.dim_target_product 
    where day_date = {p_input_date}
    group by product_id --, year_id, quarter_id
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_tmp_target_product).createOrReplaceTempView("tmp_target_product")

sql = """
        insert overwrite table edw.mid_sku_org_day_road_stock_peacebird
        partition(day_date)
        select b.product_id
            , b.product_code
            , b.color_id
            , b.color_code
            , b.size_id
            , b.size_code
            , c.store_id as org_id
            , c.store_code as org_code
            , c.store_type as org_type           
            , a.qtyprein as road_stock_qty
            , a.amt_priceqtyprein as road_stock_amt
            , a.qtypreout as on_order_qty
            , a.amt_priceqtypreout as on_order_amt
            , a.qtycan as peacebird_available_qty
            , a.amt_qtycan as peacebird_available_amt
            , current_timestamp as etl_time
            , {p_input_date} as day_date
        from peacebird.fa_storage_view a 
        inner join edw.dim_product_sku b on a.m_productalias_id = b.sku_id
        inner join tmp_target_product ttp       -- 筛选目标产品  将该表 的 数据量 由 6百万级 降到 几十万 级别
            on b.product_id = ttp.product_id
        inner join edw.dim_store c on a.c_store_id = c.store_id
        --union all 
        --select * from edw.mid_sku_org_day_road_stock_peacebird
        --where day_date <> {p_input_date}
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)

spark.catalog.dropTempView("tmp_target_product")

