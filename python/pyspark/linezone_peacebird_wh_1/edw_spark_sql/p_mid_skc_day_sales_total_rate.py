# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_skc_day_sales_total_rate.py
   Description :   skc天累计销量占比表
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

p_input_date = "'"+sys.argv[1]+"'"

# skc 日 在途 库存
sql_tmp_skc_day_road_stock = '''
    select 
        product_id, color_id 
        , sum(road_stock_qty) as road_stock_qty
    from edw.mid_sku_org_day_road_stock_peacebird
    where day_date = {p_input_date}
    group by  product_id, color_id
'''.format(**{"p_input_date": p_input_date})

#print(sql_tmp_skc_day_road_stock)

spark.sql(sql_tmp_skc_day_road_stock).createOrReplaceTempView("tmp_skc_day_road_stock")

# skc 日 总仓 库存
sql_tmp_warehouse_day_end_stock = '''
    select
        b.product_id
        ,b.color_id
        ,sum(b.stock_qty) as warehouse_total_stock_qty
    from edw.mid_day_end_stock b 
    inner join (select store_id from edw.dim_target_store where store_code = 'CB37') c-- 只取出总仓
            on b.org_id = c.store_id
    where b.stock_date = {p_input_date}
    group by b.product_id,b.color_id
'''.format(**{"p_input_date": p_input_date})

#print(sql_tmp_warehouse_day_end_stock)

spark.sql(sql_tmp_warehouse_day_end_stock).createOrReplaceTempView("tmp_warehouse_day_end_stock")

# skc 日 门店 库存
sql_tmp_store_day_end_stock = '''
    select
        b.product_id
        ,b.color_id
        ,sum(b.stock_qty) as store_total_stock_qty
    from edw.mid_day_end_stock b 
    inner join (select store_id from edw.dim_target_store where store_type = '自营' and is_store = 'Y') c
            on b.org_id = c.store_id
    where b.stock_date = {p_input_date}
    group by b.product_id,b.color_id
'''.format(**{"p_input_date": p_input_date})

#print(sql_tmp_store_day_end_stock)

spark.sql(sql_tmp_store_day_end_stock).createOrReplaceTempView("tmp_store_day_end_stock")

sql_tmp_skc_end_stock = '''
    select
        coalesce(a.product_id, b.product_id) as product_id
        , coalesce(a.color_id, b.color_id) as color_id
        , coalesce(a.warehouse_total_stock_qty, 0) as warehouse_total_stock_qty
        , coalesce(b.store_total_stock_qty, 0) as store_total_stock_qty
    from tmp_warehouse_day_end_stock a 
    full join tmp_store_day_end_stock b 
        on a.product_id = b.product_id 
        and a.color_id = b.color_id
'''

spark.sql(sql_tmp_skc_end_stock).createOrReplaceTempView("tmp_skc_end_stock")

sql_tmp_sales = '''
    SELECT
        fs.product_id 
        , fs.color_id 
        , sum(qty) as sale_total
    from edw.fct_sales fs
    inner join edw.dim_target_store ts            -- 排除 加盟商的销售数据
        on fs.store_id = ts.store_id
    inner join tmp_skc_end_stock tss
        on fs.product_id = tss.product_id
        and fs.color_id = tss.color_id 
    where fs.sale_date <= {p_input_date}
    group by fs.product_id, fs.color_id
'''.format(**{"p_input_date": p_input_date})

#print(sql_tmp_sales)

spark.sql(sql_tmp_sales).createOrReplaceTempView("tmp_sales")

sql_insert = """
    insert overwrite table edw.mid_skc_day_sales_total_rate partition(sale_date)
    select
        tss.product_id
        , tss.color_id
        --, {p_input_date} as sale_date
        , coalesce(sale.sale_total,0) as sale_total
        , ( tss.store_total_stock_qty+tss.warehouse_total_stock_qty+coalesce(tdrs.road_stock_qty,0)) as inventory_total
        , (coalesce(sale.sale_total,0)+tss.store_total_stock_qty+tss.warehouse_total_stock_qty+coalesce(tdrs.road_stock_qty,0)) as product_total
        ,case 
            when (coalesce(sale.sale_total,0)+tss.store_total_stock_qty+tss.warehouse_total_stock_qty+coalesce(tdrs.road_stock_qty,0))=0 then 0
                else (coalesce(sale.sale_total,0)*1.0/(coalesce(sale.sale_total,0)+tss.store_total_stock_qty+tss.warehouse_total_stock_qty+coalesce(tdrs.road_stock_qty,0)))
         end 
            as overate
        , current_timestamp as etl_time
        , {p_input_date} as sale_date
    from tmp_skc_end_stock tss
    left join tmp_skc_day_road_stock tdrs  --记录数1110845
        on tss.product_id = tdrs.product_id 
            and tss.color_id = tdrs.color_id
    left join tmp_sales sale
        on tss.product_id = sale.product_id 
            and tss.color_id = sale.color_id

   -- union all 

    --select * from edw.mid_skc_day_sales_total_rate
    --where sale_date <> {p_input_date}
""".format(**{"p_input_date": p_input_date})

#print(sql_insert)


spark.sql(sql_insert)

spark.catalog.dropTempView("tmp_skc_day_road_stock")
spark.catalog.dropTempView("tmp_warehouse_day_end_stock")
spark.catalog.dropTempView("tmp_store_day_end_stock")
spark.catalog.dropTempView("tmp_skc_end_stock")
spark.catalog.dropTempView("tmp_sales")



