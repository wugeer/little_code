# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_city_week_sales_change_comp
# Author: lh
# Date: 2018/10/10 15:30
# ---------------------------------------------------------------------------- 

# ------------------- 时间 问题 -------------------
#select default.sys_trunc(date_add('{p_input_date}',1),'week')
# default.sys_trunc('week', date_add('2018-10-07',1))
# default.sys_trunc('week', date_add('{p_input_date}',1))
# 取周一的数据
# 2018-10-08  airflow 调度时传入的是 2018-10-07  取模型销量增长表 的 2018-10-08 到  2018-10-08   最终表 存的是 2018-10-01
# 2018-10-09  airflow 调度时传入的是 2018-10-08  取模型销量增长表 的 2018-10-08 到  2018-10-09   最终表 存的是 2018-10-01
# ……
# 2018-10-14  airflow 调度时传入的是 2018-10-13  取模型销量增长表 的 2018-10-08 到  2018-10-14   最终表 存的是 2018-10-01 
# 2018-10-15  airflow 调度时传入的是 2018-10-14  取模型销量增长表 的 2018-10-15 到  2018-10-15   最终表 存的是 2018-10-08
#             airflow 调度时传入的是 {p_input_date}  
#              取模型销量增长表 的 default.sys_trunc(date_add('{p_input_date}',1), 'week') 到  date_add('{p_input_date}',1)   
#                           最终表 存的是 default.sys_trunc( date_sub('{p_input_date}', 6), 'week')


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #本周累计销量增长及销量损失
    sql_tmp_skc_sales_total = '''
        SELECT 
            product_code
            , color_code
            , city_code
            , city_long_code
            , sum(ai_sales_grow) as ai_sales_grow
            , sum(peacebird_sales_grow) as peacebird_sales_grow
            , sum(ai_sales_loss) as ai_sales_loss
            , sum(peacebird_sales_loss) as peacebird_sales_loss
        from {p_dm_schema}.dm_skc_city_day_sales_change_comp
        where day_date >= '{p_input_date_add_one_mon}'
            and day_date <= date_add('{p_input_date}',1)
        group by product_code, color_code, city_code, city_long_code
    '''
    
    #传入日期 + 1 的 铺货率分子 分母
    sql_tmp_skc_distribute = '''
        select 
            c.product_code
            , c.color_code
            , b.org_code as city_code
            , b.org_longcode as city_long_code
            , a.act_distribute_store_count as peacebird_distribute_rate_numerator
            , a.total_store_count as peacebird_distribute_rate_denominator
            , a.mod_distribute_store_count as ai_distribute_rate_numerator
            , a.total_store_count as ai_distribute_rate_denominator
        from {p_edw_schema}.mod_skc_city_day_sales_grow_loss a 
        inner join {p_edw_schema}.dim_stockorg b 
            on a.city_id = b.org_id
        inner join {p_edw_schema}.dim_product_skc c 
            on a.product_id = c.product_id
                and a.color_id = c.color_id
        where day_date = date_add('{p_input_date}',1)
    '''
    
    # skc city 断码率 分子 分母
    sql_tmp_skc_brokensize = '''
        SELECT 
        city_long_code
        , product_code
        , color_code
        , before_rep_allot_brokensize_count as peacebird_brokensize_numerator  --补调前实际断码率分子
        , before_rep_allot_skc_count as peacebird_brokensize_denominator       --补调前实际断码率分母
        , after_rep_allot_brokensize_count as ai_brokensize_numerator       --补调前模型断码率分子
        , after_rep_allot_skc_count as ai_brokensize_denominator               --补调前模型断码率分母
        from {p_rst_schema}.rst_skc_city_brokensize_count
        where day_date = date_sub('{p_input_date}', 6)  --上周 的断码率
    '''
    
    # skc city 汇总 宽表
    sql_tmp_skc_total = '''
        select 
            tsst.product_code
            , tsst.color_code
            , tsst.city_code
            , tsst.city_long_code
            , tsst.ai_sales_grow
            , tsst.peacebird_sales_grow
            , tsst.ai_sales_loss
            , tsst.peacebird_sales_loss
            , tsd.peacebird_distribute_rate_numerator
            , tsd.peacebird_distribute_rate_denominator
            , tsb.peacebird_brokensize_numerator        --实际断码率分子
            , tsb.peacebird_brokensize_denominator       --实际断码率分母
            , tsd.ai_distribute_rate_numerator
            , tsd.ai_distribute_rate_denominator
            , tsb.ai_brokensize_numerator                   --模型断码率分子
            , tsb.ai_brokensize_denominator                 --模型断码率分母                        
        from tmp_skc_sales_total tsst
        left join tmp_skc_distribute tsd
            on tsst.product_code = tsd.product_code
                and tsst.color_code = tsd.color_code
                and tsst.city_long_code = tsd.city_long_code
        left join tmp_skc_brokensize tsb
            on tsst.product_code = tsb.product_code
                and tsst.color_code = tsb.color_code
                and tsst.city_long_code = tsb.city_long_code
    '''
    
        #--在通过城市长编码 city_long_code 关联 {p_edw_schema}.dim_store  取 区域用户id  zone_id 时会出现数据重复,首先对这两个字段进行去重
    sql_tmp_store = '''
        select
            city_long_code
            , zone_id
            , zone_name
        from {p_edw_schema}.dim_store
        group by city_long_code, zone_id, zone_name
    '''
    
    # 年份 季节 和 吊牌价 一起在最后的时候计算
       
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_city_week_sales_change_comp partition(week_date)
        select
            tst.product_code
            , dps.product_name
            , tst.color_code
            , dps.color_name
            , dp.year_id as year 
            , dp.quarter_id as quarter
            , ds1.org_code as region_code               -- 大区编码
            , ds1.org_longcode as region_long_code      -- 大区长编码
            , ts.zone_id as zone_long_code
            , ts.zone_name as zone_name
            , tst.city_code
            , tst.city_long_code
            , tst.ai_sales_grow
            , tst.peacebird_sales_grow
            , tst.ai_sales_grow*dp.tag_price as ai_sales_amt_grow
            , tst.peacebird_sales_grow*dp.tag_price as peacebird_sales_amt_grow
            , tst.ai_sales_loss
            , tst.peacebird_sales_loss
            , (tst.ai_sales_loss*dp.tag_price) as ai_sales_amt_loss
            , (tst.peacebird_sales_loss*dp.tag_price) peacebird_sales_amt_loss
            , tst.peacebird_distribute_rate_numerator
            , tst.peacebird_distribute_rate_denominator
            , tst.peacebird_brokensize_numerator        --实际断码率分子
            , tst.peacebird_brokensize_denominator       --实际断码率分母
            , tst.ai_distribute_rate_numerator
            , tst.ai_distribute_rate_denominator
            , tst.ai_brokensize_numerator                   --模型断码率分子
            , tst.ai_brokensize_denominator                 --模型断码率分母
            , current_timestamp as etl_time
            , '{p_input_date_sub_six_mon}' as week_date                -- 上周一的日期作为对比日期
            --, '2018-09-17' as week_date
        from tmp_skc_total tst
        inner join {p_edw_schema}.dim_product_skc dps
            on tst.product_code = dps.product_code
                and tst.color_code = dps.color_code
        inner join {p_edw_schema}.dim_product dp                -- 取 年份 季节  吊牌价
            on tst.product_code = dp.product_code
        inner join {p_edw_schema}.dim_stockorg  ds
            on tst.city_long_code = ds.org_longcode
        inner join {p_edw_schema}.dim_stockorg ds1
            on ds.parent_id = ds1.org_id
        inner join tmp_store ts
            on tst.city_long_code = ts.city_long_code
    '''
    
    spark.create_temp_table(sql_tmp_skc_sales_total, 'tmp_skc_sales_total')
    spark.create_temp_table(sql_tmp_skc_distribute, 'tmp_skc_distribute')
    spark.create_temp_table(sql_tmp_skc_brokensize, 'tmp_skc_brokensize')
    spark.create_temp_table(sql_tmp_skc_total, 'tmp_skc_total')
    spark.create_temp_table(sql_tmp_store, 'tmp_store')
    
    spark.execute_sql(sql_insert)
    
    spark.drop_temp_table('tmp_store')
    spark.drop_temp_table('tmp_skc_total')
    spark.drop_temp_table('tmp_skc_brokensize')
    spark.drop_temp_table('tmp_skc_distribute')
    spark.drop_temp_table('tmp_skc_sales_total')  