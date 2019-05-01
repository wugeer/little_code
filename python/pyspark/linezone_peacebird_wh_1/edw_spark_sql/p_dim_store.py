# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_store
   Description :
   Author :       yangming
   date：          2018/8/4
-------------------------------------------------
   Change Activity:
                   2018/11/05:
-------------------------------------------------
-- 项目: peacebird
-- 过程名: p_dim_store
-- 源表: peacebird.c_store_view(店铺表)
--       peacebird.c_area_view(区域表)
--       peacebird.c_storekind_view(门店种类表)
--       peacebird.c_storetype_jz_view(门店类型表)
--       edw.dim_city_location
--       edw.dim_stockorg
-- 目标表: edw.dim_store
-- 程序描述: 门店仓库维表
-- 程序路径: /opt/peacebird/edw/p_dim_store.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v1.1 增加仓库部分
--             v2.0 alter   改写pyspark
--             v3.0 alter   增加门店长编码、城市id等字段
--             v3.1 alter   增加价格类型字段
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

# -- 添加门店部分的数据
sql_tmp1 = """
    select distinct a.id as store_id              --门店id
       , a.code as store_code                       --门店编码
       , q.org_longcode as store_long_code          --门店长编码
       , b.name as store_type                       --门店类型
       --, (case 
       --         when b.name = '自营' and a.c_storeattrib8_id = '2687' then '正价店'
       --         when b.name = '自营' a.c_storeattrib8_id = '2887' then '长特店'
       --         when b.name = '自营' a.c_storeattrib8_id = '4887' then '临特店'
       --   else '正价店'                             -- 门店为空时，处理为正价店 --2018年11月15日15:09:13
       --     end ) as price_type                     -- 价格类型      -- 2018年11月5日22:28:55
       , (case 
                when b.name = '加盟' then null            -- 由于 临特、长特 这些价格分类是用于 调拨 模型的，加盟数据目前只用于补货，所以加盟的价格类型直接置为 null
                when b.name = '自营' and a.c_storeattrib8_id = '2887' then '长特店'
                when b.name = '自营' and a.c_storeattrib8_id = '4887' then '临特店'
          else '正价店'                             -- 门店为空时，处理为正价店 --2018年11月15日15:09:13
            end ) as price_type                     -- 价格类型      -- 2018年11月5日22:28:55
       , c.name as store_kind                       --类型    
       , a.name as store_name                       --店铺名称
       , a.level as store_level                     --店铺级别
       , a.namedesc as biz_district                 --商圈
       , a.plane_bis as selling_area                --营业面积
       , a.plane_store as warehouse_area            --仓库面积       
       --, a.billdate_frist as opening_time           --开业时间
       , from_unixtime(unix_timestamp(a.billdate_frist,'yyyymmdd'),'yyyy-mm-dd') as opening_time           --开业时间
       --, a.lastbilldate as close_time               --停业时间
       , from_unixtime(unix_timestamp(a.lastbilldate,'yyyymmdd'),'yyyy-mm-dd') as close_time               --停业时间
       --, a.statename as status                     --状态
       , (case when a.billdate_frist is not null and a.lastbilldate is null
          then '正常' else '关闭' end) as status   --现在取状态的逻辑
       , d.province as province                     --省份
       , (case when d.province in ('北京', '上海', '重庆', '天津') then d.province
            else d.cityname end)  as city           --城市
       , d.citylevel as city_level                  --城市等级
       , d.country as area                          --地区
       , null as clerk_count                        --店员人数
       , a.c_express_addr as address                --详细地址
       , h.org_code as city_code                    --城市编码
       , h.org_id as city_id                        --城市id
       , h.org_longcode as city_long_code           --城市长编码
       , i.org_code as dq_code                      --大区编码
       , i.org_id as dq_id                          --大区id
       , i.org_longcode as dq_long_code             --大区长编码编码
       , k.org_code as nanz_code                    --男装编码
       , k.org_id as nanz_id                        --男装id
       , k.org_longcode as nanz_long_code           --男装长编码
   --     ,da.user_id as zone_id                    --区域id
   --     ,da.user as zone_name                     --区域名称
       , w.lat                                      --纬度
       , w.lng                                      --经度
       , a.is_toc                                   --是否toc
       , a.c_customer_id as customer_id             --所属经销商
       , ( case
                when a.c_customerup_id = '0' then  null  -- 非 加盟店  为 null
                when a.c_customerup_id = '1' then 'N'    -- 上级为太平鸟， 非跨级                
                else 'Y'  -- 上级不是太平鸟，跨级                                
            end
          ) as is_gap                               --是否跨级               
   from peacebird.c_store_view a 
   left join peacebird.c_area_view d on a.c_city_id = d.cityid
   left join peacebird.c_storekind_view b on a.c_storekind_id = b.id
   left join peacebird.c_storetype_jz_view c on a.c_storetype_jz_id = c.id 
   left join edw.dim_city_location w on d.cityname = w.areaname and d.province = w.shortname
   left join edw.dim_stockorg q on a.id = q.org_id               -- 门店
   left join edw.dim_stockorg h on q.parent_id = h.org_id        -- 城市
   left join edw.dim_stockorg i on h.parent_id = i.org_id        -- 大区
   left join edw.dim_stockorg k on i.parent_id = k.org_id        -- 男装
   --left join edw.dim_area da on d.cityname = da.area or d.province = da.area
   where ( b.name = '自营' and c.name != '电商' ) or b.name = '加盟'
"""

tmp1 = spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")

sql_1 = """
    insert overwrite table edw.dim_store
    select a.store_id                       --门店id
        , a.store_code                       --门店编码
        , a.store_long_code                  --门店编码
        , a.store_type                       --门店类型
        , a.price_type                       --价格类型
        , a.store_kind                       --类型    
        , a.store_name                       --店铺名称
        , a.store_level                      --店铺级别
        , a.biz_district                     --商圈
        , a.selling_area                     --营业面积
        , a.warehouse_area                   --仓库面积
        , a.opening_time                     --开业时间
        , a.close_time                       --停业时间
        , a.status                           --状态
        , a.province                         --省份
        , a.city                             --城市
        , a.city_level                       --城市等级
        , a.area                             --地区
        , a.clerk_count                      --店员人数
        , a.address                          --详细地址
        , a.city_code                        --城市编码
        , a.city_id                          --城市id
        , a.city_long_code                   --城市长编码
        , a.dq_code                          --大区编码
        , a.dq_id                            --大区id
        , a.dq_long_code                     --大区长编码编码
        , a.nanz_code                        --男装编码
        , a.nanz_id                          --男装id
        , a.nanz_long_code                   --男装长编码
        , ( case 
                when a.store_type = '自营'  then b.user_id   -- 只有自营门店有区域id
                else null 
            end
            ) as zone_id                     --区域id
       , ( case 
                when a.store_type = '自营'  then b.user
                else null 
            end
            ) as zone_name                   --区域名称
        , a.lat                              --纬度
        , a.lng                              --经度
        , 'Y' as is_store                    --是否门店
        , a.is_toc as is_toc                 --是否toc
        , a.customer_id                      --所属经销商
        , a.is_gap                           --是否跨级
        , CURRENT_TIMESTAMP as etl_time 
    from tmp1 as a 
    left join edw.dim_area as b on a.city = b.area
    where a.store_kind != '仓库' and a.store_name not like '%指标%'
"""

sql_2 = """
    insert into table edw.dim_store
    select distinct a.id as store_id                --门店id
        , a.code as store_code                       --门店编码
        , null as store_long_code                    --门店长编码
        , b.name as store_type                       --门店类型
        , null as price_type                         -- 价格类型    非门店 价格类型为null
        , c.name as store_kind                       --类型                                            
        , a.name as store_name                       --店铺名称
        , null as store_level                        --店铺级别
        , a.namedesc as biz_district                 --商圈
        , a.plane_bis as selling_area                --营业面积
        , a.plane_store as warehouse_area            --仓库面积
        --, a.billdate_frist as opening_time           --开业时间
        --, a.lastbilldate as close_time               --停业时间
        , from_unixtime(unix_timestamp(a.billdate_frist,'yyyymmdd'),'yyyy-mm-dd') as opening_time           --开业时间
        , from_unixtime(unix_timestamp(a.lastbilldate,'yyyymmdd'),'yyyy-mm-dd') as close_time               --停业时间
        , a.statename as status                      --状态      -- 同上面的 门店 状态的判断 不同
        , d.province as province                     --省份
        , d.cityname as city                         --城市
        , d.citylevel as city_level                  --城市等级
        , d.country as area                          --地区
        , null as clerk_count                        --店员人数
        , a.c_express_addr as address                --详细地址
        , null as city_code                          --城市编码
        , null as city_id                            --城市id
        , null as city_long_code                     --城市长编码
        , null as dq_code                            --大区编码
        , null as dq_id                              --大区id
        , null as dq_long_code                       --大区长编码编码
        , null as nanz_code                          --男装编码
        , null as nanz_id                            --男装id
        , null as nanz_long_code                     --男装长编码
        , null as zone_id                            --区域id
        , null as zone_name                          --区域名称
        , w.lat                                      --纬度
        , w.lng                                      --经度
        , 'N' as is_store                            --是否门店
        , a.is_toc as is_toc                         --扩展字段
        , a.c_customer_id as customer_id             --所属经销商
        , null as is_gap                               --是否跨级
        , CURRENT_TIMESTAMP as etl_time    
    from peacebird.c_store_view a 
    left join peacebird.c_area_view d on a.c_city_id = d.cityid            
    left join peacebird.c_storekind_view b on a.c_storekind_id = b.id        
    left join peacebird.c_storetype_jz_view c on a.c_storetype_jz_id = c.id 
    left join edw.dim_city_location w on d.cityname = w.areaname and d.province = w.shortname
    where ( (b.name != '自营' and a.name not like '%指标%') or (b.name = '自营' and c.name != '电商' and c.name = '仓库' and a.name not like '%指标%') ) 
    	and b.name <> '加盟'      -- 排除加盟的店铺
"""

sql_insert_target_store = '''
	insert overwrite table edw.dim_target_store 
	select 
        store_id, store_code, store_long_code, store_type
        , price_type, store_kind, store_name, store_level
        , biz_district, selling_area, warehouse_area
        , opening_time, close_time
        , status
        , province, city, city_level
        , area, clerk_count, address
        , city_code, city_id, city_long_code
        , dq_code, dq_id, dq_long_code
        , nanz_code, nanz_id, nanz_long_code
        , zone_id, zone_name
        , lat, lng
        , is_store, is_toc
        , current_timestamp as etl_time
  from edw.dim_store
	where store_type <> '加盟'
'''

spark.sql(sql_1)

spark.sql(sql_2)

spark.sql(sql_insert_target_store)

# drop 临时表
spark.catalog.dropTempView("tmp1")
