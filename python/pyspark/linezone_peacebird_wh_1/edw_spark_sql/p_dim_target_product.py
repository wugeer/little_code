# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_stock_check_res
   Description :
   Author :       yangming
   date：          2018/8/12
-------------------------------------------------
   Change Activity:
                   2018/8/12:
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

p_input_date = sys.argv[1] if len(sys.argv) > 1 else '2018-05-31'

params_dic = {'p_input_date': repr(p_input_date)}

sql_tmp1 = """
            select b.product_id
                , b.product_name
                , b.product_code
                , b.year_id
                , b.quarter_id
                , b.big_class
                , b.mid_class
                , b.tiny_class
                , b.mictiny_class
                , b.brand
                , b.band
                , b.cost_price
                , b.tag_price
                , b.gender
                , b.put_on_date
                , b.pull_off_date
                , a.day_date
            from edw.dim_target_quarter a 
            inner join edw.dim_product b on a.year_id = b.year_id and a.quarter_id = b.quarter_id 
            where a.day_date = {p_input_date}
""".format(**params_dic)

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")

# 写入目标产品表
sql = """
        insert overwrite table edw.dim_target_product
        partition(day_date)
        select c.org_id as dq_id
            , c.org_longcode as dq_long_code
            , null as city_id
            , null as city_long_code
            , a.product_id
            , a.product_name
            , a.product_code
            , a.year_id
            , a.quarter_id
            , a.big_class
            , a.mid_class
            , a.tiny_class
            , a.mictiny_class
            , a.brand
            , a.band
            , a.cost_price
            , a.tag_price
            , a.gender
            , a.put_on_date
            , a.pull_off_date
            , current_timestamp as etl_time
            , a.day_date
        from tmp1 as a
        left join (select *, {p_input_date} as day_date from edw.dim_stockorg where org_type = '大区') as c
            on a.day_date = c.day_date
        --union all
        --select * from edw.dim_target_product
        --where day_date <> {p_input_date}
""".format(**params_dic)

spark.sql(sql)

# drop 临时表
spark.catalog.dropTempView("tmp1")


