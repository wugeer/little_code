# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_sales
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_fct_sales
-- 源表: peacebird.m_retail(零售表头)
--      peacebird.m_retailitem(零售明细)
--      peacebird.m_attributesetinstance(颜色尺码信息)
        edw.dim_product_sku
        edw.dim_store
-- 目标表: edw.fct_sales
-- 程序描述: 销售事实表
-- 程序路径: /opt/peacebird/edw/p_fct_sales.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v1.1 修改字段取值错误
--             v2.0 alter   改写pyspark
"""


from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf

import os

warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')

# app_name
file_name = os.path.basename(__file__)
app_name = "".join(["PySpark-", file_name])

# config 配置
spark_conf = SparkConf()
spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
# 解决 分区下 小文件过多的问题
spark_conf.set("spark.sql.shuffle.partitions", '20')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
        insert overwrite table edw.fct_sales
        select null as machine_id										--机器号
            , a.modifieddate as audit_time							    --审核时间
            , a.c_store_id as store_id								    --店铺id
            , a.tot_amt_actual as pay_amt								--整单金额
            , (a.tot_amt_list - a.tot_amt_actual) as discount_amt		--整单折扣金额
            , a.id as order_id										    --订单号
            , a.c_vip_id as customer_code								--客户编码
            , null as sale_time								            --业务时间
            , from_unixtime(unix_timestamp(a.billdate,'yyyyMMdd'),'yyyy-MM-dd') as sale_date
            , b.id as serial_id										    --分录的id号
            , b.m_product_id as product_id							    --商品id
            , ma.value1_id as color_id								    --颜色id
            , ma.value2_id as size_id									--尺码id
            , b.qty as qty											    --数量
            , b.priceactual as real_price								--商品成交单价
            , b.tot_amt_actual as real_amt							    --商品成交总额
            , current_timestamp as etl_time
          from peacebird.m_retail_view a 
          left join peacebird.m_retailitem_view b on a.id = b.m_retail_id
          left join peacebird.m_attributesetinstance_view ma on b.m_attributesetinstance_id = ma.id
          inner join edw.dim_store s on a.c_store_id = s.store_id
          inner join edw.dim_product_sku h on b.m_product_id = h.product_id 
            and ma.value1_id = h.color_id and ma.value2_id = h.size_id
         where a.status = '2' and a.isactive = 'Y' and s.is_store='Y'
"""

spark.sql(sql)
