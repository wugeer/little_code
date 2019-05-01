# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_stockorg
   Description :
   Author :       yangming
   date：          2018/8/3
-------------------------------------------------
   Change Activity:
                   2018/8/3:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_stockorg
-- 源表: peacebird.c_area_view
--      peacebird.c_storekind_view
--      peacebird.c_store_view            增加加盟门店 468 家 2018年12月14日10:49:18
--      peacebird.c_storetype_jz_view
-- 目标表: edw.dim_stockorg
-- 程序描述: 库存组织表
-- 程序路径: /opt/peacebird/edw/p_dim_stockorg.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v1.1 去掉CB37仓库这一级
--             v2.0 alter   改写pyspark
--             v3.0 alter   增加加盟门店数据  2018年12月14日10:47:55
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

# --1.1、男装总部与大区的关系
sql_tmp1 = """
            select bigarea as dq_name                  --大区名称
                , bigareacode as dq_code                --大区编码
                , bigareaid as dq_id                    --大区id
                , 'B_00' as parent_code                 --父组织编码
                , 'B_11' as parent_id                   --父组织id
                , '男装' as parent_name                 --父组织名称
                , '大区' as org_type                    --组织类型
                , '3' as org_typecode                   --组织类型编码
                , bigareaisactive as dq_status          --状态
            from peacebird.c_area_view
            group by bigarea, bigareacode, bigareaid, bigareaisactive
"""

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")


# --1.2、大区与城市的关系
sql_tmp2 = """
            select cityname as city_name                    --城市名称
                , citycode as city_code                    --城市编码
                , cityid as city_id                        --城市id
                , bigareacode as parent_code                --父组织编码
                , bigareaid as parent_id                    --父组织id
                , bigarea as parent_name                     --父组织名称
                , '城市' as org_type                        --组织类型
                , '2' as org_typecode                        --组织类型编码
                , cityisactive as city_status                --状态
            from peacebird.c_area_view
            group by cityname, citycode, cityid, bigareacode, bigareaid, bigarea, cityisactive
"""
tmp2 = spark.sql(sql_tmp2).createOrReplaceTempView("tmp2")

#  --1.3、门店与城市的关系
sql_tmp3 = """
        select a.name as store_name                                        --门店名称
            , a.code as store_code                                       --门店编码
            , a.id as store_id                                           --门店id
            , (case when d.provincecode in ('11','21','22','36') 
                  then d.provincecode else d.citycode end) as parent_code       --父组织编码                         
            , (case when d.provinceid in ('111','89','114','120') 
                  then d.provinceid else d.cityid end) as  parent_id         --父组织id
            , (case when d.province in ('上海','北京','天津','重庆') 
                  then d.province else d.cityname end) as parent_name   --父组织名称
            , '门店' as org_type                                         --组织类型
            , '1' as org_typecode                                        --组织类型编码
            , (case when d.provincecode in ('11','21','22','36') 
                  then d.provincecode else d.citycode end) as citycode       --城市编码                         
            , (case when d.provinceid in ('111','89','114','120') 
                  then d.provinceid else d.cityid end) as cityid         --城市id
           , (case when a.billdate_frist is not null and a.lastbilldate is null
              then '正常' else '关闭' end) as status                        --现在取状态的逻辑
        from peacebird.c_store_view a 
        left join peacebird.c_area_view d on a.c_city_id = d.cityid
        left join peacebird.c_storekind_view b on a.c_storekind_id = b.id        
        left join peacebird.c_storetype_jz_view c on a.c_storetype_jz_id = c.id 
        where ( b.name = '自营' and c.name != '电商' and c.name != '仓库' and a.name not like '%指标%' )
            or b.name = '加盟'   -- 增加 加盟 门店
"""

tmp3 = spark.sql(sql_tmp3).createOrReplaceTempView("tmp3")

# --2.构造关系
# --2.1 组织为男装
sql_2_1 = """
            insert overwrite table edw.dim_stockorg
            select '男装' as org_name                         --组织名称
                , 'B_00' as org_code                        --组织编码
                , 'B_11' as org_id                          --组织id
                , null as org_longname                      --组织长名称
                , null as org_longcode                      --组织长编码
                , null as parent_code                       --父组织编码
                , null as parent_id                         --父组织id
                , '男装' as org_type                        --组织类型
                , '4' as org_typecode                       --组织类型编码
                , 'Y' as status                             --状态
                , null as remark                            --备注
                , current_timestamp as etl_time    
"""

# --2.2 组织为大区
sql_2_2 = """
            insert into table edw.dim_stockorg
            select distinct a.dq_name as org_name                                --组织名称
                , a.dq_code as org_code                                --组织编码
                , a.dq_id as org_id                                    --组织id
                , concat(b.org_name,':',a.dq_name) as org_longname    --组织长名称
                , concat(b.org_code,':',a.dq_code) as org_longcode    --组织长编码
                , a.parent_code                            --父组织编码
                , a.parent_id                                --父组织id
                , a.org_type                                --组织类型
                , a.org_typecode                            --组织类型编码
                , a.dq_status as status                                   --状态
                , null as remark                                        --备注    
                , current_timestamp as etl_time
            from tmp1 a 
            left join edw.dim_stockorg b on a.parent_code = b.org_code        --男装
"""

# --2.3 组织为城市
sql_2_3 = """
            insert into table edw.dim_stockorg
            select distinct b.city_name as org_name                        --组织名称
                , b.city_code as org_code                                --组织编码
                , b.city_id as org_id                                    --组织id
                , concat(c.org_name,':',a.dq_name,':',b.city_name) as org_longname    --组织长名称
                , concat(c.org_code,':',a.dq_code,':',b.city_code) as org_longcode    --组织长编码
                , a.dq_code as parent_code                            --父组织编码
                , a.dq_id as parent_id                                --父组织id
                , b.org_type as org_type                                --组织类型
                , b.org_typecode as org_typecode                            --组织类型编码
                , b.city_status as status                                    --状态
                , null as remark                                        --备注    
                , current_timestamp as etl_time
            from tmp2 b 
            left join tmp1 a on a.dq_code = b.parent_code     --大区
            left join edw.dim_stockorg c on a.parent_code = c.org_code        --男装
"""

# --2.4 组织为门店
sql_2_4 = """
            insert into table edw.dim_stockorg
            select distinct b.store_name as org_name                        --组织名称
                , b.store_code as org_code                                --组织编码
                , b.store_id as org_id                                    --组织id
                , concat(c.org_name,':',a.dq_name,':',h.city_name,':',b.store_name) as org_longname    --组织长名称
                , concat(c.org_code,':',a.dq_code,':',h.city_code,':',b.store_code) as org_longcode    --组织长编码
                , h.city_code as parent_code                                --父组织编码
                , h.city_id as parent_id                                    --父组织id
                , b.org_type as org_type                                    --组织类型
                , b.org_typecode as org_typecode                            --组织类型编码
                , b.status as status                                        --状态
                , null as remark                                            --备注    
                , current_timestamp as etl_time
            from tmp3 b 
            left join tmp2 h on h.city_code = b.parent_code    --城市
            left join tmp1 a on a.dq_code = h.parent_code    --大区
            left join edw.dim_stockorg c on a.parent_code = c.org_code        --男装
"""

# 执行spark.sql
spark.sql(sql_2_1)
spark.sql(sql_2_2)
spark.sql(sql_2_3)
spark.sql(sql_2_4)

# drop 临时表
spark.catalog.dropTempView("tmp1")
spark.catalog.dropTempView("tmp2")
spark.catalog.dropTempView("tmp3")

