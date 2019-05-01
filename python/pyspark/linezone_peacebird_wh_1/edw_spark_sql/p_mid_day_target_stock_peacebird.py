# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_day_target_stock_peacebird
   Description :
   Author :       yangming
   date：          2018/9/7
-------------------------------------------------
   Change Activity:
                   2018/9/7:
                   2108/12/04:
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

p_input_date = repr(sys.argv[1])

#  -- 取传入日期当天去重后 的 目标产品id
sql_tmp_target_product = '''
    select 
        product_id 
    from edw.dim_target_product 
    where day_date = {p_input_date}
    group by product_id --, year_id, quarter_id
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_tmp_target_product).createOrReplaceTempView("tmp_target_product")


# -- 取过去7天某skc在某门店 累计销量>=2 的skc 门店 id
#sql_tmp_sales = '''
#    select 
#        store_id
#        , fs.product_id, color_id 
#    , sum(case 
#            when fs.sale_date > date_sub({p_input_date}, 7) and fs.sale_date <= {p_input_date} then fs.qty 
#            else 0 
#          end)  
#        as last_seven_days_sales_qty
#    from edw.fct_sales fs 
#    inner join tmp_target_product ttp 
#        on fs.product_id = ttp.product_id 
#    group by store_id, fs.product_id, color_id 
#    having last_seven_days_sales_qty >= 2
#'''.format(**{"p_input_date": p_input_date})


#spark.sql(sql_tmp_sales).createOrReplaceTempView("tmp_sales")

# 取传入日期当天 日末库存 并去重
sql_tmp_end_stock = '''
    select 
        org_id
        , product_id, color_id, size_id
    from edw.mid_day_end_stock 
    where stock_date = {p_input_date}
    group by org_id, product_id, color_id, size_id
'''.format(**{"p_input_date": p_input_date})

spark.sql(sql_tmp_end_stock).createOrReplaceTempView("tmp_end_stock")

sql = """
    insert overwrite table edw.mid_day_target_stock_peacebird partition(stock_date)
    --insert overwrite table edw_temp.mid_day_target_stock_peacebird partition(stock_date)
    SELECT
        store.store_id as org_id
        , store.store_type as org_type
        , sku.product_id
        , sku.color_id
        , sku.size_id
        , cast(stock.target as int) as stock_qty
        --, stock.active as active                     -- 是否激活
        --, (case 
        --        when (stock.target is not null and stock.target <> '') then cast(stock.target as int)
        --        else 1 
        --    end ) as stock_qty
        --, (case 
        --    when (stock.target is not null and stock.target <> '') then stock.active
        --    else 'Y'
        --    end ) as active                     -- 是否激活
        --, (case 
        --    when (stock.target is not null and stock.target <> '') and tns.product_id is null then stock.active
        --    else 'Y'
        --    end ) as active                     -- 是否激活
        , (case 
                when tns.product_id is not null and cast(stock.target as int) > 0  then 'Y'   -- 只有当 初始维护的目标库存大于0 及 目标库存有记录
                else stock.active
            end ) as active                              -- 是否激活
        , 'N' as is_emp                                 -- 是否为空   现在默认为N   2018年12月19日19:59:08
        , CURRENT_TIMESTAMP as etl_time
        , {p_input_date} as stock_date 
    from peacebird.st_prod_view as stock
    inner join edw.dim_store store          -- 取组织id 及 组织类型
        on store.store_id = stock.c_store_id
    inner join tmp_target_product ttp   -- 筛选目标产品
        on stock.m_product_id = ttp.product_id
    inner join edw.dim_product_sku as sku
        on stock.skuid = sku.sku_id
    --left join tmp_sales ts 
    --    on stock.c_store_id = ts.store_id and sku.product_id = ts.product_id 
    --        and sku.color_id = ts.color_id
    left join tmp_end_stock tns
        on stock.c_store_id = tns.org_id 
            and sku.product_id = tns.product_id and sku.color_id = tns.color_id and sku.size_id = tns.size_id
    where (stock.target is not null and stock.target <> '')           -- 取目标库存不为null，并且目标库存不为空的
    --    or ( (stock.target is null or stock.target = '') and ts.product_id is not null and size_code in ('2','3','4','5') )    
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)


# drop 临时表
#spark.catalog.dropTempView("tmp_sales")
spark.catalog.dropTempView("tmp_target_product")
spark.catalog.dropTempView("tmp_end_stock")