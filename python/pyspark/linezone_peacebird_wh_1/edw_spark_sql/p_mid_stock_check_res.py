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

# 查出日末库存表
sql_tmp1 = """
            select  a.product_id, a.color_id, a.size_id, a.org_id, a.stock_qty now_qty
            from edw.mid_day_end_stock a
            where a.stock_date = {p_input_date}
""".format(**params_dic)

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")

# 查出月结库存表
sql_tmp2 = """
            select * from edw.fct_month_end_stock
            where stock_date = {p_input_date}
""".format(**params_dic)

tmp2 = spark.sql(sql_tmp2).createOrReplaceTempView("tmp2")

# 关联上的条数
sql_tmp3 = """
            select max(c.store_code) org_code, max(c.store_name) org_name, max(c.store_type) org_type
                , sum(case when a.now_qty=b.stock_qty then 1 else 0 end) equal_record_total								
                , count(1) our_record_total																			
                , count(b.product_id) their_record_total															
            from tmp1 a 
            left join tmp2 b on a.product_id=b.product_id 
                and a.color_id=b.color_id and a.size_id=b.size_id and a.org_id=b.org_id
            inner join edw.dim_store c on a.org_id=c.store_id
            group by a.org_id
"""

tmp3 = spark.sql(sql_tmp3).createOrReplaceTempView("tmp3")

# 关联不上为0的条数

sql_tmp4 = """
            select max(c.store_code) org_code 
                , count(1) not_join_total
                , sum(case a.now_qty when 0 then 1 else 0 end) not_join_and_equal_0
            from tmp1 a 
            left join tmp2 b on a.product_id=b.product_id
                and a.color_id=b.color_id and a.size_id=b.size_id and a.org_id=b.org_id 
            inner join edw.dim_store c on a.org_id=c.store_id 
            where b.product_id is null
            group by a.org_id
"""

tmp4 = spark.sql(sql_tmp4).createOrReplaceTempView("tmp4")

sql_res = """
            insert overwrite table edw.mid_stock_check_res
            select {p_input_date} stock_date
                , a.org_code 
                , a.org_name
                , a.org_type
                , a.our_record_total
                , a.their_record_total
                , a.equal_record_total
                , (a.equal_record_total*1.0/(case a.their_record_total when 0 then null else a.their_record_total end)) equal_rate
                , b.not_join_total
                , b.not_join_and_equal_0
                , (b.not_join_and_equal_0*1.0/(case b.not_join_total when 0 then null else b.not_join_total end)) equal_0_rate
                , now()
            from tmp3 a 
            left join tmp4 b on a.org_code=b.org_code
            union all 
            select * from edw.mid_stock_check_res
            where stock_date <> {p_input_date}
""".format(**params_dic)

spark.sql(sql_res)

# drop 临时表
spark.catalog.dropTempView("tmp1")
spark.catalog.dropTempView("tmp2")
spark.catalog.dropTempView("tmp3")
spark.catalog.dropTempView("tmp4")

