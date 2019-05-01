# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_org_day_target_amt.py
   Description :
   Author :       zsm
   date：          2018/8/6
-------------------------------------------------
   Change Activity:
                   2018/8/6:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_org_day_target_amt.py
-- 源表: 
-- 目标表:mid_org_day_target_amt
-- 程序描述: 各维度每天的目标营业额
-- 程序路径: /opt/peacebird/edw_spark_sql/p_mid_org_day_target_amt.py
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
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

# 0 目标营业额表有重复的，先去重
sql = """
select distinct c_store_id,year,week_of_year,tot_amt_target
from peacebird.c_store_biwktarget
where tot_amt_target!='0'
"""
spark.sql(sql).createOrReplaceTempView("c_store_biwktarget_tmp")

# 1、获取门店本月每周目标营业额：门店，每周开始时间，每周结束时间，每周总营业额
sql = """
select  
    store.store_id
    ,store.store_code
    ,beginer.date_string as begin_date
    ,ender.date_string as end_date
    ,cast(target.tot_amt_target as decimal(30,8)) as target_amt
from c_store_biwktarget_tmp as target
inner join edw.dim_target_amt_date as beginer
    on cast(target.year as int)=beginer.year_id
    and cast(target.week_of_year as int)=beginer.begin_date_week_id
inner join edw.dim_target_amt_date as ender
    on cast(target.year as int)=ender.year_id
    and cast(target.week_of_year as int)=ender.end_date_week_id
inner join edw.dim_store as store
    on target.c_store_id=store.store_id
where
    ender.date_string<=last_day({p_input_date})
    and beginer.date_string>=trunc({p_input_date},'MM')
""".format(**{"p_input_date": p_input_date})

spark.sql(sql).createOrReplaceTempView("tmp1")

# 2、统计了每个组织本月每周目标营业额：组织，组织类型，每周开始时间，每周结束时间，每周总营业额
sql = """
select
    tmp1.store_id as org_id
    ,tmp1.store_code as org_code
    ,store.org_longcode as org_long_code
    ,'门店' as org_type
    ,tmp1.begin_date
    ,tmp1.end_date
    ,tmp1.target_amt
from tmp1
inner join edw.dim_stockorg as store
    on tmp1.store_code=store.org_code
union all
select
    min(org2.org_id) as org_id
    ,min(org2.org_code) as org_code
    ,org2.org_longcode as org_long_code
    ,'城市' as org_type
    ,tmp1.begin_date as begin_date
    ,tmp1.end_date as end_date
    ,sum(tmp1.target_amt) as target_amt
from tmp1
inner join edw.dim_stockorg as org
    on tmp1.store_code=org.org_code
inner join edw.dim_stockorg as org2
    on org.parent_id=org2.org_id
group by org2.org_longcode,tmp1.begin_date,tmp1.end_date
union all
select
    zone.zone_id as zone_id
    ,zone.zone_id as org_code
    ,zone.zone_id as org_long_code
    ,'区域' as org_type
    ,tmp1.begin_date as begin_date
    ,tmp1.end_date as end_date
    ,sum(tmp1.target_amt) as target_amt
from tmp1
inner join (select store_code,zone_id from edw.dim_store as store where is_store='Y') as zone
    on tmp1.store_code=zone.store_code
group by zone.zone_id,tmp1.begin_date,tmp1.end_date
union all
select
    min(org3.org_id) as org_id
    ,min(org3.org_code) as org_code
    ,org3.org_longcode as org_long_code
    ,'大区' as org_type
    ,tmp1.begin_date as begin_date
    ,tmp1.end_date as end_date
    ,sum(tmp1.target_amt) as target_amt
from tmp1
inner join edw.dim_stockorg as org
    on tmp1.store_code=org.org_code
inner join edw.dim_stockorg as org2
    on org.parent_id=org2.org_id
inner join edw.dim_stockorg as org3
    on org2.parent_id=org3.org_id
group by org3.org_longcode,tmp1.begin_date,tmp1.end_date
union all
select
    min(org4.org_id)
    ,org4.org_code as org_code
    ,org4.org_code as org_long_code
    ,'男装' as org_type
    ,tmp1.begin_date as begin_date
    ,tmp1.end_date as end_date
    ,sum(tmp1.target_amt) as target_amt
from tmp1
inner join edw.dim_stockorg as org
    on tmp1.store_code=org.org_code
inner join edw.dim_stockorg as org2
    on org.parent_id=org2.org_id
inner join edw.dim_stockorg as org3
    on org2.parent_id=org3.org_id
inner join edw.dim_stockorg as org4
    on org3.parent_id=org4.org_id
group by org4.org_code,tmp1.begin_date,tmp1.end_date

"""

spark.sql(sql).createOrReplaceTempView("tmp2")
spark.catalog.dropTempView("tmp1")

# 3、获取每个组织本月每周每天的目标营业额（暂时还只是本周总营业额，只是为了方便后面计算），
# 组织，组织类型，每周开始时间，每周结束时间，每周总营业额
sql = """
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,tmp2.begin_date,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,1) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,2) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,3) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,4) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,5) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
union all
select tmp2.org_id,tmp2.org_code,tmp2.org_long_code,tmp2.org_type
    ,date_add(tmp2.begin_date,6) as begin_date
    ,tmp2.end_date,tmp2.target_amt
from tmp2
"""
spark.sql(sql).createOrReplaceTempView("tmp")
# print("tmp are as follows:")
# print(spark.sql("select * from tmp limit 10").show())
spark.catalog.dropTempView("tmp2")

# 4、从第三步中找出符合条件的日期的记录（未考虑权重）
sql = """
select *
from tmp
where
    tmp.begin_date<=tmp.end_date
"""
spark.sql(sql).createOrReplaceTempView("tmp22")
spark.catalog.dropTempView("tmp")

# 5、第四步的结果append一个权重（去年的营业额数据计算的）
# 组织，组织类型，每周开始时间，每周结束时间，每周总营业额*重新分配的权重=每天营业额
sql = """
insert overwrite table edw.mid_org_day_target_amt
partition(day_date)
select 
    tmp22.org_id,tmp22.org_code,tmp22.org_long_code,tmp22.org_type
    ,(tmp22.target_amt)
     *(case when b.weight is not null and b.weight !=0 then b.weight else c.weight end)
     /sum(case when b.weight is not null and b.weight!=0 then b.weight else c.weight end) over(partition by (tmp22.org_long_code,tmp22.end_date)) as target_amt   
    ,current_timestamp as etl_time
    ,tmp22.begin_date as day_date
from tmp22
inner join edw.dim_date a
    on tmp22.begin_date=a.date_string
left join (select * from edw.dim_org_week_day_sales_weight where ref_year_id=year(trunc({p_input_date},'MM'))-1) as b
    on a.weekday_id=b.week_day
    and tmp22.org_id=b.org_id
inner join (select * from edw.dim_org_week_day_sales_weight where ref_year_id=year(trunc({p_input_date},'MM'))-1 and org_id='B_11') as c
    on a.weekday_id=c.week_day
--union all
--select * from edw.mid_org_day_target_amt
--where
--    day_date>last_day({p_input_date})
--    or day_date<trunc({p_input_date},'MM')
""".format(**{"p_input_date": p_input_date})

spark.sql(sql)
spark.catalog.dropTempView("tmp22")


