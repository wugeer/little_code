# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_store_skc_trans_status.py
   Description :
   Author :       lh
   date：          2018/12/09
-------------------------------------------------
   Change Activity:
                   2018/12/09:
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

sql_target_product = '''
    select 
        product_id
    from edw.dim_target_product 
    where day_date = {p_input_date} 
    --where day_date = '2019-01-09'
    group by product_id
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_target_product).createOrReplaceTempView("target_product")

# 取不补货的门店
sql_tmp_store_n = '''
    select 
        a.C_STORE_ID as store_id
    from peacebird.st_pause_store_item_view a 
    left join peacebird.st_pause_area_view b on a.st_pause_area_id = b.id
    left join edw.dim_store d on a.c_store_id = d.store_id
    inner join peacebird.st_joinstore_view c on a.c_store_id = c.c_store_id
    where b.is_all = 'Y'
        and b.status = '2'
        and (b.close_status = '1' or b.close_status is null)
        and b.isactive = 'Y'
        and b.begindate <FROM_UNIXTIME(unix_timestamp(cast(now() as string),  'yyyy-MM-dd'),  'yyyyMMdd')
        and (b.enddate > FROM_UNIXTIME(unix_timestamp(cast(now() as string),  'yyyy-MM-dd'),  'yyyyMMdd')
            or b.enddate is null)
        and a.isactive = 'Y'
'''

spark.sql(sql_tmp_store_n).createOrReplaceTempView("tmp_store_n")

# 取 目标产品的skc 
sql_tmp_skc = '''
    select 
        b.product_id
        , b.color_id
    from target_product a
    inner join edw.dim_product_skc b 
        on a.product_id = b.product_id
'''
spark.sql(sql_tmp_skc).createOrReplaceTempView("tmp_skc")


sql_tmp_store_skc = '''
    -- # 取 不补货门店和 skc的笛卡尔积
    select 
        store_id
        , product_id
        , color_id
        , '1' as not_in
        , '0' as not_out
    from tmp_store_n
    cross join tmp_skc

    union all 

    -- # 不补货的门店和目标产品的组合

    select 
        c.c_store_id as store_id
        , d.product_id
        , d.color_id
        , '1' as not_in
        , '0' as not_out
    from peacebird.st_pause_product_item_view a
    inner join target_product tp 
        on a.m_product_id = tp.product_id 
    left join peacebird.st_pause_area_view b 
        on a.st_pause_area_id = b.id
    left join peacebird.st_pause_store_item_view c 
        on c.st_pause_area_id = b.id
    inner join edw.dim_product_skc d 
        on a.m_product_id = d.product_id
    where (b.is_all <> 'Y' or b.is_all = null)
        and b.status = '2'
        and (b.close_status = '1' or b.close_status is null)
        and b.isactive = 'Y'
        and b.begindate <FROM_UNIXTIME(unix_timestamp(cast(now() as string),  'yyyy-MM-dd'),  'yyyyMMdd')
        and (b.enddate > FROM_UNIXTIME(unix_timestamp(cast(now() as string),  'yyyy-MM-dd'),  'yyyyMMdd')
                or b.enddate is null)
        and a.isactive = 'Y'
        and c.isactive = 'Y'
        and c.c_store_id is not null 
    group by c.c_store_id, d.product_id, d.color_id
'''
spark.sql(sql_tmp_store_skc).createOrReplaceTempView("tmp_store_skc")

# 不补货的门店和目标产品  去重

sql_tmp_store_skc_last = '''
    select 
        store_id
        , product_id
        , color_id
        , not_in
        , not_out
    from tmp_store_skc
    group by store_id, product_id, color_id, not_in, not_out 
'''
spark.sql(sql_tmp_store_skc_last).createOrReplaceTempView("tmp_store_skc_last")

sql = """
    insert overwrite table edw.dim_store_skc_trans_status   
    partition (date_dec)

    select 
        store_id
        , product_id
        , color_id
        , not_in
        , not_out
        , current_timestamp as etl_time 
        , date_add({p_input_date}, 1) as date_dec       -- 决策日期
    from tmp_store_skc_last
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)

spark.catalog.dropTempView("target_product")
spark.catalog.dropTempView("tmp_store_n")
spark.catalog.dropTempView("tmp_skc")
spark.catalog.dropTempView("tmp_store_skc")
spark.catalog.dropTempView("tmp_store_skc_last")



