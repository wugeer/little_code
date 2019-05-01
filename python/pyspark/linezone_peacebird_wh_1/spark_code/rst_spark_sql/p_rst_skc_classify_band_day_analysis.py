# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     rst_skc_classify_band_day_analysis
   Description :
   Author :       yangming
   date：          2018/9/12
-------------------------------------------------
   Change Activity:
                   2018/9/12:
-------------------------------------------------
"""

import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 创建tmp_target_product. 目标产品
    sql_tmp_target_product = """
        select product_id
            , min(year_id) as year_id
            , min(quarter_id) as quarter_id
            , min(band) as band
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id    
    """

    # 创建tmp_skc_total_sales表， 计算单个skc的累计销量
    sql_tmp_skc_total_sales = """
        select a.product_code,a.color_code
            , p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , a.total_sales_qty
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_country_day_sales as a 
        inner join {p_edw_schema}.dim_product as b on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc as dps on a.product_code = dps.product_code and a.color_code = dps.color_code
        inner join (select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}') as p
            on dps.product_id = p.product_id and dps.color_id = p.color_id
        where a.day_date = '{p_input_date}'
    """

    # 创建tmp_skc_distributed表， 单个skc的上市天数、skc数
    sql_tmp_skc_distributed = """
        select product_code,color_code,distributed_days as on_sales_days
        from {p_dm_schema}.dm_skc_country_day_life_cycle 
        where day_date = '{p_input_date}' 
    """

    # 创建tmp_skc_sell_rate， 单个skc的售卖速度
    sql_tmp_skc_sell_rate = """
        select a.product_code,a.color_code,a.prod_class,a.stage_lifetime,a.year_id,a.quarter_id
            ,a.band,a.day_date
            ,(a.total_sales_qty*1.0 / b.on_sales_days) as skc_selling_rate
        from tmp_skc_total_sales a 
        left join tmp_skc_distributed b on a.product_code = b.product_code and a.color_code= b.color_code    
    """

    # 创建tmp_sell_rate， 售卖速度=单个skc的售卖速度之和/skc数
    sql_tmp_sell_rate = """
        select a.prod_class,a.stage_lifetime,a.year_id,a.quarter_id,a.band,a.day_date
            ,(sum(a.skc_selling_rate) / count(1)) as selling_rate
        from tmp_skc_sell_rate a 
        left join tmp_skc_distributed b on a.product_code = b.product_code and a.color_code = b.color_code 
        group by a.prod_class, a.stage_lifetime, a.year_id, a.quarter_id, a.band,a.day_date    
    """

    # 创建tmp_cla_band_sales，累计销量、前7天累计销量
    sql_tmp_cla_band_sales = """
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id
            , band
            , total_sales_qty
            , last_seven_days_sales_qty
        from {p_dm_schema}.dm_skc_classify_band_country_day_sales
        where day_date = '{p_input_date}'  
    """

    # 创建tmp_cla_band_stock, 分类波段的库存
    sql_tmp_cla_band_stock = """
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id
            , band
            , warehouse_total_stock_qty
            , store_total_stock_qty
            , road_total_stock_qty
        from {p_dm_schema}.dm_skc_classify_band_country_day_stock
        where day_date = '{p_input_date}'    
    """

    # 创建tmp_cb37_road_stock, 计算总仓CB37的在途库存
    sql_tmp_cb37_road_stock = """
        select p.prod_class
            , p.stage_lifetime
            , h.year_id
            , h.band
            , h.quarter_id
            , sum(coalesce(a.road_stock_qty,0)) as warehouse_road_stock_qty
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a 
        inner join tmp_target_product h on a.product_id = h.product_id
        inner join {p_edw_schema}.dim_product_skc c on a.product_id = c.product_id and a.color_id = c.color_id
        inner join (select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}') as p
            on c.product_id = p.product_id and c.color_id = p.color_id
        where a.org_code = 'CB37' and a.day_date = '{p_input_date}'
        group by p.prod_class,p.stage_lifetime,h.year_id,h.band,h.quarter_id    
    """

    # 创建tmp_cla_band_has_sales， 有销量门店数
    sql_tmp_cla_band_has_sales = """
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id
            , band
            , has_sales_store_qty
        from {p_dm_schema}.dm_skc_classify_band_country_day_has_sales
        where day_date = '{p_input_date}'    
    """

    # 创建tmp_cla_band_dis， 断码数，铺货门店数
    sql_tmp_cla_band_dis = """
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id
            , band
            , brokensize_count
            , distributed_store_count
        from {p_dm_schema}.dm_skc_classify_band_country_day_available_distributed
        where day_date = '{p_input_date}'    
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_classify_band_day_analysis
        partition(day_date)
        select c.prod_class
            , c.stage_lifetime
            , c.year_id as year
            , c.quarter_id as quarter
            , c.band
            , (b.total_sales_qty*1.0 / (b.total_sales_qty + c.warehouse_total_stock_qty + h.warehouse_road_stock_qty 
                + c.store_total_stock_qty + c.road_total_stock_qty)) as sales_out_rate      -- 售罄率
            , (b.last_seven_days_sales_qty*1.0 / d.has_sales_store_qty) as avg_sales_qty    -- 七天销量/七天有销量的门店数
            , a.selling_rate                                                                -- 售卖速度
            , (f.brokensize_count*1.0 / f.distributed_store_count) as brokensize_rate       -- 断码率
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from tmp_cla_band_stock c
        left join tmp_cla_band_sales b on c.prod_class = b.prod_class and c.stage_lifetime = b.stage_lifetime
            and c.year_id = b.year_id and c.quarter_id = b.quarter_id and c.band = b.band
        left join tmp_sell_rate a on a.prod_class = c.prod_class and a.stage_lifetime = c.stage_lifetime
            and a.year_id = c.year_id and a.quarter_id = c.quarter_id and a.band = c.band
        left join tmp_cb37_road_stock h on c.prod_class = h.prod_class and c.stage_lifetime = h.stage_lifetime
            and c.year_id = h.year_id and c.quarter_id = h.quarter_id and c.band = h.band
        left join tmp_cla_band_has_sales d on c.prod_class = d.prod_class and c.stage_lifetime = d.stage_lifetime
            and c.year_id = d.year_id and c.quarter_id = d.quarter_id and c.band = d.band
        left join tmp_cla_band_dis f on c.prod_class = f.prod_class and c.stage_lifetime = f.stage_lifetime
            and c.year_id = f.year_id and c.quarter_id = f.quarter_id and c.band = f.band
    """

    tmp_target_product = foo.create_temp_table(sql_tmp_target_product, 'tmp_target_product')
    tmp_skc_total_sales = foo.create_temp_table(sql_tmp_skc_total_sales, 'tmp_skc_total_sales')
    tmp_skc_distributed = foo.create_temp_table(sql_tmp_skc_distributed, 'tmp_skc_distributed')
    tmp_skc_sell_rate = foo.create_temp_table(sql_tmp_skc_sell_rate, 'tmp_skc_sell_rate')
    tmp_sell_rate = foo.create_temp_table(sql_tmp_sell_rate, 'tmp_sell_rate')
    tmp_cla_band_sales = foo.create_temp_table(sql_tmp_cla_band_sales, 'tmp_cla_band_sales')
    tmp_cla_band_stock = foo.create_temp_table(sql_tmp_cla_band_stock, 'tmp_cla_band_stock')
    tmp_cb37_road_stock = foo.create_temp_table(sql_tmp_cb37_road_stock, 'tmp_cb37_road_stock')
    tmp_cla_band_has_sales = foo.create_temp_table(sql_tmp_cla_band_has_sales, 'tmp_cla_band_has_sales')
    tmp_cla_band_dis = foo.create_temp_table(sql_tmp_cla_band_dis, 'tmp_cla_band_dis')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp_target_product')
    foo.drop_temp_table('tmp_skc_total_sales')
    foo.drop_temp_table('tmp_skc_distributed')
    foo.drop_temp_table('tmp_skc_sell_rate')
    foo.drop_temp_table('tmp_sell_rate')
    foo.drop_temp_table('tmp_cla_band_sales')
    foo.drop_temp_table('tmp_cla_band_stock')
    foo.drop_temp_table('tmp_cb37_road_stock')
    foo.drop_temp_table('tmp_cla_band_has_sales')
    foo.drop_temp_table('tmp_cla_band_dis')
