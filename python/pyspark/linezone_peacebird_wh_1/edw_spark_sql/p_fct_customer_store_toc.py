# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_customer_store_toc
   Description :
   Author :       lh
   date：          2019/01/05
-------------------------------------------------
   Change Activity:
                   2019/01/05:
-------------------------------------------------
-- 项目: peacebird
-- 过程名: p_fct_customer_store_toc
-- 源表: peacebird.st_customer_view(ST参与经销商表)
--       peacebird.v_fa_customer_view(经销商统计信息表)

-- 目标表: edw.fct_customer_store_toc
-- 程序描述: st参与经销商金额相关信息
-- 程序路径: /opt/peacebird/edw_spark_sql/p_fct_customer_store_toc.py
-- 程序备注:  
-- 版本号信息: v1.0 create
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

# -- 通过 c_customer_id 关联取 取当前可提货金额



sql_insert = '''
	   insert overwrite table edw.fct_customer_store_toc 
  	 select 
      scv.c_customer_id as customer_id
      , ds.store_id
      , scv.discount
      , scv.fund_pass
      , vv.feecantake
      , {p_input_date} as day_date
      , current_timestamp as etl_time
    from peacebird.st_customer_view scv 
    inner join peacebird.v_fa_customer_view vv
        on scv.c_customer_id = vv.c_customer_id
    inner join edw.dim_store ds 
        on scv.c_customer_id = ds.customer_id

    union all 
    select * from edw.fct_customer_store_toc
    where day_date <> {p_input_date}
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_insert)