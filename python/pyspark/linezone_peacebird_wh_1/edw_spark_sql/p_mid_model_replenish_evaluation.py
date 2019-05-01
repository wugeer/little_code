# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_model_replenish_evaluation
   Description :
   Author :       yangming
   date：          2018/8/23
-------------------------------------------------
   Change Activity:
                   2018/8/23:
-------------------------------------------------
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dateutil.parser import parse

import os
import sys

warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')

# app_name
file_name = os.path.basename(__file__)
app_name = "".join(["PySpark-", file_name])

p_input_date = sys.argv[1]

res_dic = {"p_input_date": parse(p_input_date).strftime("%Y-%m-%d")}

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

# -- 取出实际总仓的发货，需要关联目标产品
sql_tmp1 = """
                select a.product_id
                    , a.color_id
                    , a.receive_org_id as store_id
                    , sum(a.send_qty) as peacebird_rep_qty
                from edw.fct_stock a 
                inner join (select * from edw.dim_store where is_store='Y' and status='正常') b 
                    on a.receive_org_id = b.store_id
                inner join (select * from edw.dim_target_product where day_date='{p_input_date}') c 
                  on a.product_id = c.product_id and b.dq_long_code = c.dq_long_code
                where a.send_date = '{p_input_date}' and a.send_org_id = '420867'
                group by a.product_id, a.color_id, a.receive_org_id
""".format(**res_dic)

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")

# -- 取出模型做出的补货
sql_tmp2 = """
                select a.product_id
                    , a.color_id
                    , a.receive_store_id as store_id
                    , sum(a.send_qty) as ai_rep_qty
                from edw.mod_sku_day_replenish a 
                inner join (select * from edw.dim_store where is_store='Y' and status='正常') b 
                    on a.receive_store_id = b.store_id 
                where a.date_send = '{p_input_date}' and a.send_org_id = '420867'
                group by a.product_id, a.color_id, a.receive_store_id
""".format(**res_dic)

tmp2 = spark.sql(sql_tmp2).createOrReplaceTempView("tmp2")

sql_tmp3 = """
                select coalesce(a.product_id, b.product_id) as product_id
                    , coalesce(a.color_id, b.color_id) as color_id
                    , coalesce(a.store_id, b.store_id) as store_id
                    , coalesce(b.ai_rep_qty, 0) as ai_rep_qty
                    , coalesce(a.peacebird_rep_qty, 0) as peacebird_rep_qty
                from tmp1 a 
                full join tmp2 b on a.product_id = b.product_id
                    and a.color_id = b.color_id and a.store_id = b.store_id
"""

tmp3 = spark.sql(sql_tmp3).createOrReplaceTempView("tmp3")

sql = """
        insert overwrite table edw.mid_model_replenish_evaluation
        partition(day_date)
        select a.product_id
            , b.product_code
            , a.color_id
            , b.color_code
            , '420867' as org_id
            , 'CB37' as org_code
            , a.store_id
            , c.store_code
            , a.ai_rep_qty
            , a.peacebird_rep_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp3 a 
        inner join edw.dim_product_skc b on a.product_id = b.product_id
            and a.color_id = b.color_id 
        inner join edw.dim_store c on a.store_id = c.store_id
""".format(**res_dic)

spark.sql(sql)

# drop 临时表
spark.catalog.dropTempView("tmp1")
spark.catalog.dropTempView("tmp2")
spark.catalog.dropTempView("tmp3")
