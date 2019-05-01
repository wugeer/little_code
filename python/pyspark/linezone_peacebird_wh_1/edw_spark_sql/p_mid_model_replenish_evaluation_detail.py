# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_model_replenish_evaluation_detail
   Description :
   Author :       yangming
   date：          2018/9/5
-------------------------------------------------
   Change Activity:
                   2018/9/5:
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
                    , a.size_id
                    , a.receive_org_id as store_id
                    , sum(a.send_qty) as peacebird_rep_qty
                from edw.fct_stock a 
                inner join (select * from edw.dim_store where is_store='Y' and status='正常') b 
                    on a.receive_org_id = b.store_id
                inner join (select * from edw.dim_target_product where day_date='{p_input_date}') c 
                  on a.product_id = c.product_id and b.dq_long_code = c.dq_long_code
                where a.send_date = '{p_input_date}' and a.send_org_id = '420867'
                group by a.product_id, a.color_id, a.size_id, a.receive_org_id
""".format(**res_dic)

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")

# -- 取出模型做出的补货
sql_tmp2 = """
                select a.product_id
                    , a.color_id
                    , a.size_id
                    , a.receive_store_id as store_id
                    , sum(a.send_qty) as ai_rep_qty
                from edw.mod_sku_day_replenish a 
                inner join (select * from edw.dim_store where is_store='Y' and status='正常') b 
                    on a.receive_store_id = b.store_id 
                where a.date_send = '{p_input_date}' and a.send_org_id = '420867'
                group by a.product_id, a.color_id, a.size_id, a.receive_store_id
""".format(**res_dic)

tmp2 = spark.sql(sql_tmp2).createOrReplaceTempView("tmp2")

sql_tmp3 = """
                select coalesce(a.product_id, b.product_id) as product_id
                    , coalesce(a.color_id, b.color_id) as color_id
                    , coalesce(a.size_id, b.size_id) as size_id
                    , coalesce(a.store_id, b.store_id) as store_id
                    , coalesce(b.ai_rep_qty, 0) as ai_rep_qty
                    , coalesce(a.peacebird_rep_qty, 0) as peacebird_rep_qty
                from tmp1 a 
                full join tmp2 b on a.product_id = b.product_id and a.color_id = b.color_id 
                  and a.size_id = b.size_id and a.store_id = b.store_id
"""

tmp3 = spark.sql(sql_tmp3).createOrReplaceTempView("tmp3")

sql_tmp4 = """
                select product_id
                    , color_id
                    , size_id
                    , store_id
                    , sum(case when sale_date >= date_sub('{p_input_date}', 7) then qty
                              else cast(0 as int) end) as last_w1_sales_qty
                    , sum(case when sale_date < date_sub('{p_input_date}', 7) then qty 
                              else cast(0 as int) end) as last_w2_sales_qty
                from edw.fct_sales
                where sale_date >= date_sub('{p_input_date}', 14) and sale_date <= date_sub('{p_input_date}', 1)
                group by product_id, color_id, size_id, store_id 
""".format(**res_dic)

tmp4 = spark.sql(sql_tmp4).createOrReplaceTempView("tmp4")
# print(spark.sql('select * from tmp4 limit 10').show())

sql = """
        insert overwrite table edw.mid_model_replenish_evaluation_detail
        partition(day_date)
        select a.product_id
            , b.product_code
            , a.color_id
            , b.color_code
            , a.size_id
            , b.size_code
            , b.size_name
            , '420867' as org_id
            , 'CB37' as org_code
            , a.store_id
            , c.store_code
            , d.stock_qty as init_stock_qty
            , e.target_stock as target_stock_inventory
            --, cast(f.last_w1_sales_qty as int) as last_w1_sales_qty
            --, cast(f.last_w2_sales_qty as int) as last_w2_sales_qty
            , f.last_w1_sales_qty
            , f.last_w2_sales_qty
            , a.ai_rep_qty
            , a.peacebird_rep_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp3 a
        inner join edw.dim_product_sku b on a.product_id = b.product_id
            and a.color_id = b.color_id and a.size_id = b.size_id
        inner join edw.dim_store c on a.store_id = c.store_id
        left join (select * from edw.mid_day_end_stock where stock_date = date_sub('{p_input_date}', 1)) d
            on a.product_id = d.product_id and a.color_id = d.color_id
                and a.size_id = d.size_id and a.store_id = d.org_id
        left join (select * from edw.mod_sku_store_day_target_inventory where day_date = '{p_input_date}') e
            on a.product_id = e.product_id and a.color_id = e.color_id
                and a.size_id = e.size_id and a.store_id = e.store_id
        left join tmp4 f on a.product_id = f.product_id and a.color_id = f.color_id
                and a.size_id = f.size_id and a.store_id = f.store_id
""".format(**res_dic)

spark.sql(sql)

# drop 临时表
spark.catalog.dropTempView("tmp1")
spark.catalog.dropTempView("tmp2")
spark.catalog.dropTempView("tmp3")
spark.catalog.dropTempView("tmp4")

