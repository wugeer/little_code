# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_four_week_sales_assess
# Author: lh
# Date: 2019年1月15日13:47:03
# change: 2019年1月17日15:50:46  新增 正确率字段
# change: 2019年1月18日10:51:05  新增已售卖周数字段
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
  
    # 取 28天前的模型四周销量预测
    sql_tmp_skc_four_week_sales_pre = '''
        select
            product_id
            , color_id
            , four_week_sales_qty
        from {p_edw_schema}.mod_skc_day_sales_prediction
        where pre_day_date = date_sub('{p_input_date}', 28-1)   -- 当天凌晨跑的销量预测，存的是当天的日期，用的是T-1 的数据
    '''

    # 取 28天前的日末库存>0的门店为可比门店
    sql_tmp_comp_store = '''
        select 
            mds.org_id as comp_store_id  -- 可比门店
        from {p_edw_schema}.mid_day_end_stock mds
        inner join edw.dim_target_store dts   -- -- 排除加盟店的影响
            on mds.org_id = dts.store_id
        where mds.stock_date = date_sub('{p_input_date}', 28) and mds.stock_qty > 0
        group by org_id
    '''

    # 计算 近 28天的实际销量和可比门店的销量
    sql_tmp_skc_for_week_sales_act = '''
        select 
            fs.product_id
            , fs.color_id
            , sum(fs.qty) as four_week_sales_qty_act -- 4周实际销量
            , sum(
                    case 
                        when tcs.comp_store_id is not null then fs.qty
                        else 0
                    end
                ) as four_week_sales_qty_comp_store -- 4周可比门店销量
        from {p_edw_schema}.fct_sales fs
        inner join edw.dim_target_store dts    -- 排除加盟店的影响
            on fs.store_id = dts.store_id
        left join tmp_comp_store tcs
            on fs.store_id = tcs.comp_store_id 
        where fs.sale_date > date_sub('{p_input_date}', 28) 
            and fs.sale_date <= '{p_input_date}'
        group by fs.product_id, fs.color_id
    '''

    # 取 28天前的 已售卖天数 因为 模型 28天的做的预测是和 售卖天数相关的
    sql_tmp_distributed_days = '''
        select
            product_code
            , color_code
            , distributed_days 
        from {p_dm_schema}.dm_skc_country_day_pre_classify
        where day_date = date_sub('{p_input_date}', 28)
    '''

    # 取传入日期当天的 skc断码率
    sql_tmp_skc_avg_brokensize_rate = '''
        select
            a.product_code
            , a.color_code
            , avg(a.brokensize_rate) as avg_brokensize_rate
        from {p_dm_schema}.dm_skc_country_day_brokensize_rate a
        where day_date > date_sub('{p_input_date}', 28)  and day_date <= '{p_input_date}'
        group by a.product_code, a.color_code
    '''

    # 将数据 插入到 目标中, 已销量预测表为主表

    # 准确率的定义 1 - abs(四周销量预测 - 四周可比门店实际销量)/四周可比门店销量
    #             res = 1 - abs(a - b)/b
    #             当 b = 0                       res = 0
    #             当 abs(a - b)>=0  及 a>=2*b    res = 0
    #             else                           res = 1 - abs(a - b)/b
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_four_week_sales_assess
        select
            dps.product_id
            , dps.product_code
            , dps.color_id
            , dps.color_code
            , tsp.four_week_sales_qty as four_week_sales_qty_pre
            , tsa.four_week_sales_qty_act
            , tsa.four_week_sales_qty_comp_store
            , (
                case
                    when tsa.four_week_sales_qty_comp_store = 0 
                        or tsp.four_week_sales_qty >= 2*tsa.four_week_sales_qty_comp_store  then 0
                    else 1 - abs(tsp.four_week_sales_qty - tsa.four_week_sales_qty_comp_store)/tsa.four_week_sales_qty_comp_store
                end
                ) as accuracy    -- 准确率
            , dpc.distributed_days as distributed_days -- 已售卖周数
            , tbr.avg_brokensize_rate as avg_brokensize_rate        
            , date_sub('{p_input_date}', 28) as assess_date
            , current_timestamp as etl_time
        from tmp_skc_four_week_sales_pre tsp
        left join tmp_skc_for_week_sales_act tsa
            on tsp.product_id = tsa.product_id
                and tsp.color_id = tsa.color_id
        inner join {p_edw_schema}.dim_product_skc dps        		--取skc product_id color_id
            on tsp.product_id = dps.product_id 
            and tsp.color_id = dps.color_id
        left join tmp_distributed_days dpc   -- 取已售卖天数
            on dps.product_code = dpc.product_code
            and dps.color_code = dpc.color_code
        left join tmp_skc_avg_brokensize_rate tbr -- 取四周平均断码率
            on dps.product_code = tbr.product_code
            and dps.color_code = tbr.color_code

        union all

        select * 
        from {p_rst_schema}.rst_skc_four_week_sales_assess
        where assess_date <> date_sub('{p_input_date}', 28)
    '''

    # 创建临时表
    spark.create_temp_table(sql_tmp_skc_four_week_sales_pre, 'tmp_skc_four_week_sales_pre')
    spark.create_temp_table(sql_tmp_comp_store, 'tmp_comp_store')
    spark.create_temp_table(sql_tmp_skc_for_week_sales_act, 'tmp_skc_for_week_sales_act')
    spark.create_temp_table(sql_tmp_distributed_days, 'tmp_distributed_days') 
    spark.create_temp_table(sql_tmp_skc_avg_brokensize_rate, 'tmp_skc_avg_brokensize_rate')

    spark.execute_sql(sql_insert)

    # 删除临时表
    spark.drop_temp_table('tmp_skc_four_week_sales_pre')
    spark.drop_temp_table('tmp_comp_store')
    spark.drop_temp_table('tmp_skc_for_week_sales_act')
    spark.drop_temp_table('tmp_distributed_days')
    spark.drop_temp_table('tmp_skc_avg_brokensize_rate')
