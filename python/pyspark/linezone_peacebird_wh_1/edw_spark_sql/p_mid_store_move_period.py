# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_store_move_period.py
   Description :   门店每天补货状态表
   Author :       lh
   date：          2018/12/27
-------------------------------------------------
   Change Activity:
                   2018/12/27:
-------------------------------------------------
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime, timedelta
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

p_input_date = sys.argv[1]

# 传入日期 加 1天
p_input_date_add_one = datetime.strptime(p_input_date, "%Y-%m-%d")+ timedelta(days=1)

# 获取星期几 星期一  1  星期二 2  int
weekday_id = p_input_date_add_one.isoweekday()

# 将 1 行 7 列 数据拆成 7行 7 列

# -- 将 周一到周日 用 1…… 7 拼接起来
sql_tmp_joinstore = '''
    select 
        c_store_id, 
        concat(monday , tuesday
                      , wednesday
                      , thursday
                      , friday
                      , saturday
                      , sunday) as con_1    
    from peacebird.st_joinstore_view
'''

spark.sql(sql_tmp_joinstore).createOrReplaceTempView("tmp_joinstore")

# -- 取 可提货金额>=资格线 的 经销商  '{p_input_date}'

sql_tmp_customer = '''
    select 
        customer_id
    from edw.fct_customer_store_toc
    where day_date = '{p_input_date}' 
        and feecantake >= fund_pass 
    group by customer_id
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_tmp_customer).createOrReplaceTempView("tmp_customer")

#  hhivesql 函数 str_to_map 将 字符串拆成 map
#  lateral view explode 将 拆成多行

sql_insert = """
    insert overwrite table edw.mid_store_move_period
    -- 取 自营门店
    select 
        date_add('{p_input_date}', 1) as day_date
        , a.c_store_id
        , current_timestamp as etl_time 
    from tmp_joinstore a
    inner join edw.dim_store ds 
        on a.c_store_id = ds.store_id 
    where substr(a.con_1, {weekday_id}, 1) = 'N'   -- 只取 补货的门店  'N'表示 补货
        and ds.store_type = '自营'

    UNION ALL

    -- 取 满足条件的加盟门店
    select 
        date_add('{p_input_date}', 1) as day_date
        , a.c_store_id
        , current_timestamp as etl_time 
    from tmp_joinstore a
    inner join edw.dim_store ds 
        on a.c_store_id = ds.store_id
    inner join tmp_customer tc
        on ds.customer_id = tc.customer_id
    where substr(a.con_1, {weekday_id}, 1) = 'Y'   -- 只取 补货的门店  'Y'表示 补货 加盟

    union all 
    select * from edw.mid_store_move_period
    where day_date <> date_add('{p_input_date}', 1)
""".format(**{"weekday_id": weekday_id, "p_input_date":p_input_date})

print(sql_insert)


spark.sql(sql_insert)

spark.catalog.dropTempView("tmp_joinstore") 

spark.catalog.dropTempView("tmp_customer") 



