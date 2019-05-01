# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_city_summary
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

    # 创建tmp_target_product表， 取出全国维度的目标产品
    sql_tmp_target_product = """
        select product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    """

    # 创建tmp_sales_weeks, 取出已售卖周数
    sql_tmp_sales_weeks = """
        select product_code
            , color_code
            , sales_weeks
        from {p_dm_schema}.dm_skc_country_day_sales_weeks
        where day_date = '{p_input_date}'
    """

    # tmp_country_sales, 五周销量，正价率等
    sql_tmp_country_sales = """
        select product_code
            , color_code
            , total_sales_qty
            , concat('(', week_sales_qty, ')-'
            , last_week_sales_qty, '-'
                , last_two_week_sales_qty, '-'
                , last_three_week_sales_qty, '-'
                , last_four_week_sales_qty, '-'
                , last_five_week_sales_qty) as five_weeks_sales_trend
            , last_week_sales_qty
            , total_sales_amt
            , fullprice_total_amt
            , total_tag_price
        from {p_dm_schema}.dm_skc_country_day_sales
        where day_date = '{p_input_date}'
    """

    # tmp_warehouse_road_stock, 计算总仓CB37的在途库存
    sql_tmp_warehouse_road_stock = """
        select c.product_code,c.color_code
            ,sum(coalesce(road_stock_qty,0)) as warehouse_road_stock_qty
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a 
        inner join tmp_target_product on a.product_id = tmp_target_product.product_id
        inner join {p_edw_schema}.dim_product_skc c on a.product_id = c.product_id and a.color_id = c.color_id
        where a.org_code = 'CB37' and a.day_date = '{p_input_date}'
        group by c.product_code,c.color_code 
    """

    # 取出总仓CB37的在库库存和门店总库存
    sql_tmp_country_stock = """
        with t1 as 
        (select * from {p_dm_schema}.dm_skc_country_day_stock where day_date = '{p_input_date}'),
        t2 as 
        (select * from {p_dm_schema}.dm_skc_country_day_road_stock where day_date = '{p_input_date}')
        select coalesce(a.product_code, b.product_code) as product_code
            , coalesce(a.color_code, b.color_code) as color_code
            , coalesce(a.warehouse_total_stock_qty,0) as warehouse_stock_qty
            , coalesce(a.store_total_stock_qty,0) as store_total_stock_qty
            , coalesce(b.road_stock_qty,0) as store_total_road_stock_qty
        from t1 a 
        full join t2 b on a.product_code = b.product_code and a.color_code = b.color_code 
    """

    # 总仓库存 = 在库库存 + 在途库存
    sql_tmp_warehouse_total_stock = """
        select a.product_code,a.color_code
           ,a.warehouse_stock_qty + coalesce(b.warehouse_road_stock_qty,0) as warehouse_available_stock_qty
        from tmp_country_stock a 
        left join tmp_warehouse_road_stock b on a.product_code = b.product_code and a.color_code = b.color_code
    """

    # otb排名
    sql_tmp_otb_info = """
        select product_code
            , color_code
            , total_otb_order_qty
            , total_otb_order_rank
            , total_otb_order_in_tinyclass_rank
        from {p_dm_schema}.dm_skc_country_order
    """

    # 小类销量
    sql_tmp_tiny_class_sales = """
        select year_id
            , quarter_id
            , tiny_class
            , total_sales_qty
            , fullprice_total_amt
            , total_sales_amt
            , total_tag_price
        from {p_dm_schema}.dm_tiny_class_country_day_sales
        where day_date = '{p_input_date}'
    """

    # 小类库存
    sql_tmp_tiny_class_stock = """
        select year_id
            , quarter_id
            , tiny_class
            , warehouse_total_stock_qty as warehouse_stock_qty
            , store_total_stock_qty as store_total_stock_qty
            , road_total_stock_qty as store_total_road_stock_qty
        from {p_dm_schema}.dm_tiny_class_country_day_stock
        where day_date = '{p_input_date}'
    """

    # 取出CB11仓库的库存
    sql_tmp_cb11_stock = """
        select b.product_code
            , b.color_code
            , sum(a.stock_qty) as cb11_stock_qty
        from {p_edw_schema}.mid_day_end_stock a 
        inner join {p_edw_schema}.dim_product_skc b on a.product_id = b.product_id
            and a.color_id = b.color_id
        where a.org_id = '227859' and a.stock_date = '{p_input_date}'
        group by b.product_code, b.color_code    
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_city_summary
        partition(day_date)
        select c.product_code
            , dp.product_name
            , c.color_code
            , g.color_name
            , dp.year_id as year
            , dp.quarter_id as quarter
            , dp.tag_price
            , a.sales_weeks 
            , coalesce(b.total_sales_qty,0) as total_sales_qty --没有累积销量为0
            , coalesce(b.five_weeks_sales_trend,'(0)-0-0-0-0-0') as five_weeks_sales_trend --过去五周没有销量为0
            , (h.store_total_stock_qty+h.store_total_road_stock_qty)/b.last_week_sales_qty 
                as turnover_weeks --过去一周没有销量为null
            , c.warehouse_available_stock_qty as warehouse_total_stock_qty--总仓+总仓在途 
            , i.cb11_stock_qty
            , d.total_otb_order_qty
            , d.total_otb_order_rank
            , d.total_otb_order_in_tinyclass_rank
            , coalesce(b.total_sales_qty,0)/(h.store_total_stock_qty+
                            h.store_total_road_stock_qty+coalesce(b.total_sales_qty,0)) as prod_delivery_sales_rate
            , b.total_sales_amt/b.total_tag_price as prod_discount
            , coalesce(b.fullprice_total_amt,0)/b.total_sales_amt as prod_fullprice_rate
            , coalesce(e.total_sales_qty,0)/(f.store_total_stock_qty+
                        f.store_total_road_stock_qty+coalesce(e.total_sales_qty,0)) as tinyclass_delivery_sales_rate
            , e.total_sales_amt/e.total_tag_price as tinyclass_discount --过去没有累积销量为null
            , coalesce(e.fullprice_total_amt,0)/e.total_sales_amt as tinyclass_fullprice_rate --过去没有累积销量为null
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp_warehouse_total_stock c 
        inner join tmp_country_stock h on c.product_code = h.product_code and c.color_code = h.color_code
        inner join {p_edw_schema}.dim_product dp on c.product_code = dp.product_code
        left join tmp_country_sales b on c.product_code = b.product_code and c.color_code = b.color_code
        left join tmp_sales_weeks a on a.product_code = c.product_code and a.color_code = c.color_code
        left join tmp_otb_info d on c.product_code = d.product_code and c.color_code=d.color_code
        left join tmp_tiny_class_sales e on dp.year_id = e.year_id and dp.quarter_id = e.quarter_id
            and dp.tiny_class = e.tiny_class
        left join tmp_tiny_class_stock f on dp.year_id = f.year_id and dp.quarter_id = f.quarter_id
            and dp.tiny_class = f.tiny_class 
        left join tmp_cb11_stock i on c.product_code = i.product_code and c.color_code = i.color_code
        inner join {p_edw_schema}.dim_product_skc g on c.product_code = g.product_code and c.color_code = g.color_code
    """

    tmp_target_product = foo.create_temp_table(sql_tmp_target_product, 'tmp_target_product')
    tmp_sales_weeks = foo.create_temp_table(sql_tmp_sales_weeks, 'tmp_sales_weeks')
    tmp_country_sales = foo.create_temp_table(sql_tmp_country_sales, 'tmp_country_sales')
    tmp_warehouse_road_stock = foo.create_temp_table(sql_tmp_warehouse_road_stock, 'tmp_warehouse_road_stock')
    tmp_country_stock = foo.create_temp_table(sql_tmp_country_stock, 'tmp_country_stock')
    tmp_warehouse_total_stock = foo.create_temp_table(sql_tmp_warehouse_total_stock, 'tmp_warehouse_total_stock')
    tmp_otb_info = foo.create_temp_table(sql_tmp_otb_info, 'tmp_otb_info')
    tmp_tiny_class_sales = foo.create_temp_table(sql_tmp_tiny_class_sales, 'tmp_tiny_class_sales')
    tmp_tiny_class_stock = foo.create_temp_table(sql_tmp_tiny_class_stock, 'tmp_tiny_class_stock')
    tmp_cb11_stock = foo.create_temp_table(sql_tmp_cb11_stock, 'tmp_cb11_stock')

    # 执行sql
    foo.execute_sql(sql)

    foo.drop_temp_table('tmp_target_product')
    foo.drop_temp_table('tmp_sales_weeks')
    foo.drop_temp_table('tmp_country_sales')
    foo.drop_temp_table('tmp_warehouse_road_stock')
    foo.drop_temp_table('tmp_country_stock')
    foo.drop_temp_table('tmp_warehouse_total_stock')
    foo.drop_temp_table('tmp_otb_info')
    foo.drop_temp_table('tmp_tiny_class_sales')
    foo.drop_temp_table('tmp_tiny_class_stock')
    foo.drop_temp_table('tmp_cb11_stock')
