# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_product
   Description :
   Author :       yangming
   date：          2018/8/3
-------------------------------------------------
   Change Activity:
--
-- 项目: peacebird
-- 过程名: p_dim_product
-- 源表: peacebird.m_product_view
--       peacebird.m_dim_view
-- 目标表: edw.dim_product
-- 程序描述: 产品维表
-- 程序路径: /opt/peacebird/edw/p_dim_product.sql
-- 程序备注: 
-- 版本号信息: v1.0 create
--             v2.0 alter   去掉颜色id和尺码id
--             v3.0 alter   增加条件m_dim5_id = '4805'表示成衣
--             v4.0 alter   选择斯文,生活和男装合作，剔除成衣中的女装
--             v5.0 alter   增加>=15年的时间筛选和细分小类字段
--             v6.0 alter   改写pyspark
-------------------------------------------------
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
spark_conf.set("spark.sql.shuffle.partitions", '1')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
       insert overwrite table edw.dim_product
       select  mp.id as product_id --货号
             , mp.value as product_name --名称
             , mp.name as product_code --产品编码
             , md2.attribname as year_id --年份
             , md6.attribname as quarter_id --季节
             , md3.attribname as big_class --大类
             , null as mid_class --中类
             , md4.attribname as tiny_class --小类
             , coalesce(md23.attribname, md4.attribname)  as mictiny_class --若取不到细分小类，就取小类字段
             , md1.attribname as brand --品牌
             , md8.attribname as band --波段
             , mp.precost as cost_price --成本价
             , mp.pricelist as tag_price --吊牌价
             , '男' as gender --性别
             , date_format(mp.marketdate, 'yyyy-MM-dd') as put_on_date --上架日期 无 marketdate
             , null as pull_off_date --下架日期
             , 1 as size_group_id
             , current_timestamp as etl_time
         from peacebird.m_product_view mp
    left join peacebird.m_dim_view md2 on mp.m_dim2_id = md2.id and md2.dimflag = 'DIM2'
    left join peacebird.m_dim_view md6 on mp.m_dim6_id = md6.id and md6.dimflag = 'DIM6'
    left join peacebird.m_dim_view md3 on mp.m_dim3_id = md3.id and md3.dimflag = 'DIM3'
    left join peacebird.m_dim_view md4 on mp.m_dim4_id = md4.id and md4.dimflag = 'DIM4'
    left join peacebird.m_dim_view md1 on mp.m_dim1_id = md1.id and md1.dimflag = 'DIM1'
    left join peacebird.m_dim_view md8 on mp.m_dim8_id = md8.id and md8.dimflag = 'DIM8'
    left join peacebird.m_dim_view md23 on mp.m_dim23_id = md23.id and md23.dimflag = 'DIM23'
        where mp.m_dim1_id = '4570'     --男装
          and mp.m_dim5_id = '4805'         --成衣
          and mp.m_dim10_id in ('4776','4775','12595') -- 斯文,生活和男装合作剔除成衣中的女装
          and md2.attribname >= 2015                   -- 只取2015年后的产品
"""

spark.sql(sql)

