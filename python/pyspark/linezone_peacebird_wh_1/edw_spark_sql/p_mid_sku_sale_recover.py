# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_sku_sale_recover.py
   Description :
   Author :       lh
   date：          2018/12/17
-------------------------------------------------
   Change Activity:
                   2018/12/17:
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
p_end_date = "'" + sys.argv[2] + "'"

sql_tmp_end_stock = '''
    select 
        a.org_id
        , a.product_id, a.color_id, a.size_id
        , a.stock_qty
        , a.stock_date
    from edw.mid_day_end_stock a 
    inner join edw.dim_store b 
        on a.org_id = b.store_id
    --where a.stock_date = {p_input_date}
    where a.stock_date >= {p_input_date} and a.stock_date < {p_end_date}
        and b.is_store = 'Y'     -- 取门店的数据，不取仓库的
'''.format(**{"p_input_date": p_input_date, "p_end_date": p_end_date})

spark.sql(sql_tmp_end_stock).createOrReplaceTempView("tmp_end_stock")

sql_tmp_sale = '''
    select 
        store_id 
        , product_id, color_id,size_id
        , sum(qty) as qty
        , min(sale_date) as sale_date
    from edw.fct_sales
    --where sale_date = {p_input_date}
    where sale_date >= {p_input_date} and sale_date < {p_end_date}
    group by store_id ,product_id,color_id,size_id
'''.format(**{"p_input_date": p_input_date, "p_end_date": p_end_date,})

spark.sql(sql_tmp_sale).createOrReplaceTempView("tmp_sale")

sql = """
    insert overwrite table edw.mid_sku_sale_recover
    partition(stock_date)
    select 
        a.org_id as store_id 
        , a.product_id, a.color_id, a.size_id 
        , a.stock_qty as end_stock_qty
        , (case
                when b.product_id is not null then b.qty 
                else 0
            end ) as qty   -- 销量
        , dd.weekday_id as `dayofweek`
        , (case
                when mhi.current_date is null then 'N'
                else 'Y'
            end ) as holiyday   --是否节假日
        , current_timestamp as etl_time
        , a.stock_date
    from  tmp_end_stock a 
    left join tmp_sale b 
        on a.org_id = b.store_id 
            and a.product_id = b.product_id
            and a.color_id = b.color_id
            and a.size_id = b.size_id
            and a.stock_date = b.sale_date
    inner join edw.dim_date dd 
        on a.stock_date = dd.date_date
    left join edw.mod_holiday_info mhi 
        on a.stock_date = mhi.current_date
"""

spark.sql(sql)

spark.catalog.dropTempView("tmp_end_stock")
spark.catalog.dropTempView("tmp_sale")



