# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_store_trans_status.py
   Description :
   Author :       lh
   date：          2018/12/05
-------------------------------------------------
   Change Activity:
                   2018/12/05:
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

sql_tmp_store_trans_status = '''
    select 
        sas.store_code
        , sas.store_name
        , (case 
                when sas.store_status = '只进不出' then '01'
                when sas.store_status = '只出不进' then '10'
                when sas.store_status = '不进不出' then '11'
                else null
            end ) as not_in_and_out
    from peacebird.store_allot_status sas 
    where sas.effective = 'true'   
        and length(sas.store_status) = 4       -- 门店状态的开始和结束日期 包含 当天
        and sas.start_date <= date_add({p_input_date}, 1)
        and coalesce(sas.end_date, '9999-12-31') > {p_input_date}    -- 如果结束日期为空，就转化为'9999-12-31'

'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_tmp_store_trans_status).createOrReplaceTempView("tmp_store_trans_status")

sql = """
    insert overwrite table edw.dim_store_trans_status   -- 这张表数据量较少，建成textfile 格式，不采用parquet 格式
    
    select 
        ds.store_id
        , sas.store_code
        , sas.store_name
        , substr(not_in_and_out, 1, 1) as not_in
        , substr(not_in_and_out, 2, 1) as not_out
        , date_add({p_input_date}, 1) as date_dec       -- 决策日期
        , current_timestamp as etl_time 
    from tmp_store_trans_status sas 
    inner join edw.dim_store ds 
        on sas.store_code = ds.store_code

    union all 

    select 
        * 
    from edw.dim_store_trans_status
    where date_dec <> date_add({p_input_date}, 1)
    
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)

spark.catalog.dropTempView("tmp_store_trans_status")



