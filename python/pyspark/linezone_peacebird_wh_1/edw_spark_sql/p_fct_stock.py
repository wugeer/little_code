# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_stock
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_fct_stock
-- 源表:  peacebird.m_outitem_view
--        peacebird.m_out_view
--        peacebird.m_initem_view
--        peacebird.m_in_view
--        peacebird.m_attributesetinstance_view
--        edw.dim_product_sku
-- 目标表: edw.fct_stock
-- 程序描述: 库存移动事实表
-- 程序路径: /opt/peacebird/edw/p_fct_stock.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v1.1 增加m_initem的关联，以及修改字段取值问题
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

# -- 创建out部分
sql_m_out_part = """
            select m.docno
                , mo.m_product_id as product_id
                , ma.value1_id as color_id
                , ma.value2_id as size_id
                , m.c_orig_id as send_org_id
                , m.c_dest_id as receive_org_id
                , null as mid_org_id
                , m.billtype as movetype_code
                , mo.qtyout as send_qty
                , mo.tot_amtout_list as send_amt
                , m.dateout as send_time
                , from_unixtime(unix_timestamp(m.dateout,'yyyyMMdd'),'yyyy-MM-dd') as send_date
                , m.datein as receive_time
                , from_unixtime(unix_timestamp(m.datein,'yyyyMMdd'),'yyyy-MM-dd') as receive_date
                , m.description as out_description
            from peacebird.m_outitem_view mo 
            inner join peacebird.m_out_view m on mo.m_out_id = m.id
            inner join peacebird.m_attributesetinstance_view ma on mo.m_attributesetinstance_id = ma.id
            inner join edw.dim_product_sku dps on mo.m_product_id = dps.product_id
                and ma.value1_id = dps.color_id and ma.value2_id = dps.size_id
            where m.status = '2' and m.isactive = 'Y'
"""

m_out_part = spark.sql(sql_m_out_part).createOrReplaceTempView("m_out_part")

# -- 创建in部分
sql_m_in_part = """
            select m.docno                                                                  -- 能关联上的docno                                              
                , mi.m_product_id as product_id
                , ma.value1_id as color_id
                , ma.value2_id as size_id
                , m.c_store_id as send_org_id
                , m.c_dest_id as receive_org_id
                , null as mid_org_id
                , m.billtype as movetype_code
                , mi.qtyin as receive_qty
                , mi.tot_amtin_list as receive_amt
                , m.dateout as send_time
                , from_unixtime(unix_timestamp(m.dateout,'yyyyMMdd'),'yyyy-MM-dd') as send_date
                , m.datein as receive_time
                , from_unixtime(unix_timestamp(m.datein,'yyyyMMdd'),'yyyy-MM-dd') as receive_date
                , m.description as in_description
            from peacebird.m_initem_view mi 
            inner join peacebird.m_in_view m on mi.m_in_id = m.id
            inner join peacebird.m_attributesetinstance_view ma on mi.m_attributesetinstance_id = ma.id
            inner join edw.dim_product_sku dps on mi.m_product_id = dps.product_id
                and ma.value1_id = dps.color_id and ma.value2_id = dps.size_id
            where m.status = '2' and m.isactive = 'Y'
"""

m_in_part = spark.sql(sql_m_in_part).createOrReplaceTempView("m_in_part")

sql = """
        insert overwrite table edw.fct_stock
        select mop.docno as doc_id
            , mop.product_id as product_id
            , mop.color_id as color_id
            , mop.size_id as size_id
            , mop.send_org_id as send_org_id
            , mop.receive_org_id as receive_org_id
            , null as mid_org_id
            , mop.movetype_code as movetype_code
            , mop.send_qty as send_qty
            , mop.send_amt as send_amt
            , mip.receive_qty as receive_qty
            , mip.receive_amt as receive_amt
            , mop.send_time as send_time
            , mop.send_date as send_date
            , mop.receive_time as receive_time
            , mop.receive_date as receive_date
            , mop.out_description 
            , mip.in_description
            , current_timestamp as etl_time
        from m_out_part mop 
        left join m_in_part mip on mop.docno = mip.docno
            and mop.product_id = mip.product_id and mop.color_id = mip.color_id 
            and mop.size_id = mip.size_id     
"""

spark.sql(sql)

spark.catalog.dropTempView("m_out_part")
spark.catalog.dropTempView("m_in_part")

