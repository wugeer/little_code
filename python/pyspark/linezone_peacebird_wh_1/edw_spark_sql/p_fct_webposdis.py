# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_fct_webposdis
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/8/4:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_fct_webposdis
-- 源表: peacebird.c_webposdis_view
-- 目标表: edw.fct_webposdis
-- 程序描述: 事件活动事实表
-- 程序路径: /opt/peacebird/edw/p_fct_webposdis.sql
-- 程序备注:
-- 版本号信息: v1.0 create
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
spark_conf.set("spark.sql.shuffle.partitions", '1')

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=spark_conf) \
    .enableHiveSupport() \
    .getOrCreate()

sql = """
        insert overwrite table edw.fct_webposdis
        select cwpd.id as act_id
            , cwpd.docno as act_code
            , cwpd.name as act_name
            , cwpd.webtypename as act_type
            -- , cwpd.datebegin as act_date_begin
            -- , cwpd.dateend as act_date_end
            , from_unixtime(unix_timestamp(cwpd.datebegin,'yyyyMMdd'),'yyyy-MM-dd') as act_date_begin 
            , from_unixtime(unix_timestamp(cwpd.dateend,'yyyyMMdd'),'yyyy-MM-dd') as act_date_end
            , cwpd.timelimit as act_time_limit
            , cwpd.timebegin as act_time_begin
            , cwpd.timeend as act_time_end
            , cwpd.viplimit as vip_lime
            , cwpd.vipbirdlimit as vip_birdthday_limit
            , cwpd.vipbirmlimit as vip_birdthmonth_limit
            , cwpd.is_list_limit as is_list_limit
            , cwpd.distypename as act_off_type_name
            , cwpd.execontent as execute_content
            , cwpd.autodouble as auto_double
            , cwpd.limitmon as monday
            , cwpd.limitthe as tuesday
            , cwpd.limitwed as wendnesay
            , cwpd.limitthu as thursday
            , cwpd.limitfri as friday
            , cwpd.limitsat as saturday
            , cwpd.limitsun as sunday
            , cwpd.exdispro as exdispro
            , cwpd.ifexe_same as ifexe_same
            , cwpd.ifexe as ifexe
            , cwpd.isvipexp as isvipexp
            , cwpd.islimitpro as islimitpro
            , cwpd.limitpropricename as limitpropricename
            , cwpd.limitproqty as limitproqty
            , cwpd.dissorttypename as dissorttypename
            , cwpd.allstore as all_store
            , cwpd.description as description
            , cwpd.c_vipactplan_id as act_vip_plan_id
            , cwpd.oano as oa_number
            , cwpd.ownerid as owner_id
            , cwpd.modifierid as modifier_id
            , cwpd.creationdate as creation_date
            , cwpd.modifieddate as modified_date
            , cwpd.status as status
            , cwpd.statusname as status_name
            , cwpd.statuserid as submiter_id
            , cwpd.statustime as submit_time
            , cwpd.isactive as act_is_active
            , cwpd.writetime as write_time
            , current_timestamp as etl_time  
        from peacebird.c_webposdis_view as cwpd
"""

spark.sql(sql)
