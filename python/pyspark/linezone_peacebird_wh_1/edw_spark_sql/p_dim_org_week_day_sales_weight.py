# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dim_org_week_day_sales_weight.py
   Description :
   Author :      zsm
   date：          2018/8/6
-------------------------------------------------
   Change Activity:
                   2018/8/6:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dim_org_week_day_sales_weight.py
-- 源表: dim_org_week_day_sales_weight
-- 目标表:  
-- 程序描述: 目标营业额权重
-- 程序路径: /opt/peacebird/edw_spark_sql/p_dim_org_week_day_sales_weight.py
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""
import os
import sys
import datetime
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dateutil.parser import parse


# 调用命令：spark2-submit p_dim_org_week_day_sales_weight.py 2018-01-01会乡下执行
# 调用命令：spark2-submit p_dim_org_week_day_sales_weight.py 2018-01-02不会乡下执行
# 日期
p_input_date = parse(sys.argv[1])
p_year_start = repr(datetime.date(p_input_date.year - 1, 1, 1).strftime('%Y-%m-%d'))
p_year_end = repr(datetime.date(p_input_date.year - 1, 12, 31).strftime('%Y-%m-%d'))

if p_input_date.month == 1 and p_input_date.day == 1:
    warehouse_location = abspath('hdfs://master:9000/user/hive/warehouse')
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
        SELECT 
            a.store_code
            , a.product_code
            , a.color_code
            , a.sales_amt
            , dater.weekday_id as zhouji 
        FROM edw.mid_skc_store_day_sales as a 
        inner join edw.dim_date as dater
            on dater.date_string = a.day_date
        where 
            --a.day_date >= '2017-01-01'
            --and a.day_date <= '2017-12-31'
            a.day_date >= {p_year_start}
            and a.day_date <= {p_year_end}
    """.format(**{"p_year_start": p_year_start, "p_year_end": p_year_end})
    print(sql)

    spark.sql(sql).createOrReplaceTempView("tmp1")

    sql = """
        select 
            tmp1.store_code
            , tmp1.zhouji
            , sum(tmp1.sales_amt) as amt
        from tmp1 
        group by 
            tmp1.zhouji
            , tmp1.store_code
    """
    spark.sql(sql).createOrReplaceTempView("tmp2")

    sql = """
        select 
            tmp1.store_code
            , sum(tmp1.sales_amt) as amt
        from tmp1
        group by 
            tmp1.store_code
    """
    spark.sql(sql).createOrReplaceTempView("tmp3")

    sql = """
        --全国维度
        select 
            tmp2.store_code
            , (case when tmp3.amt = 0 then null else tmp2.amt / tmp3.amt end)as quanzhong
            , tmp2.zhouji
        from tmp2
        inner join tmp3 
            on tmp2.store_code = tmp3.store_code
    """
    spark.sql(sql).createOrReplaceTempView("tmp4")

    sql = """
        select 
            zhouji
            , avg(quanzhong) quanzhong 
        from tmp4 
        group by 
            zhouji
    """
    spark.sql(sql).createOrReplaceTempView("tmp5")

    sql = """
        insert overwrite table edw.dim_org_week_day_sales_weight
        select
            'B_11' as org_id 
            , 'B_00' as org_code
            , null as org_long_code
            , '男装' as org_type
            , zhouji as week_day
            , quanzhong as weight
            , year({p_year_start}) as ref_year_id
            , current_timestamp as etl_time
        from tmp5
        union all
        select *
        from edw.dim_org_week_day_sales_weight
        where 
            ref_year_id <> year({p_year_start})
    """.format(**{"p_year_start": p_year_start})

    spark.sql(sql)

    sql = """
        --城市维度
        insert into edw.dim_org_week_day_sales_weight
        --(org_id,org_code,org_long_code,org_type,week_day,weight,ref_year_id,etl_time)
        select 
            min(b.org_id) as org_id
            , min(b.org_code) as org_code
            , b.org_longcode as org_long_code
            , '城市' as org_type
            , tmp2.zhouji as week_day 
            , (case when sum(tmp3.amt) = 0 then null else sum(tmp2.amt) / sum(tmp3.amt) end) as weight
            , year({p_year_start}) as ref_year_id
            , current_timestamp as etl_time
        from tmp2
        inner join tmp3 
            on tmp2.store_code = tmp3.store_code
        inner join edw.dim_stockorg a 
            on tmp2.store_code = a.org_code
        inner join edw.dim_stockorg b 
            on a.parent_id = b.org_id
        group by 
            b.org_longcode,tmp2.zhouji
    """.format(**{"p_year_start": p_year_start})

    spark.sql(sql)

    sql = """
        --区域维度
        insert into edw.dim_org_week_day_sales_weight
        select
            a.zone_id as org_id
            , a.zone_id as org_code
            , a.zone_id as org_long_code
            , '区域' as org_type
            , tmp2.zhouji as week_day
            , (case when sum(tmp3.amt) = 0 then null else sum(tmp2.amt) / sum(tmp3.amt) end) as weight
            , year({p_year_start}) as ref_year_id
            , current_timestamp as etl_time
        from tmp2
        inner join tmp3
            on tmp2.store_code = tmp3.store_code
        inner join edw.dim_store a
            on tmp2.store_code = a.store_code
        group by
            a.zone_id,tmp2.zhouji
    """.format(**{"p_year_start": p_year_start})

    spark.sql(sql)

    sql = """
        --大区维度
        insert into edw.dim_org_week_day_sales_weight
        select 
            min(c.org_id) as org_id
            , min(c.org_code) as org_code
            , c.org_longcode as org_long_code
            , '大区' as org_type
            , tmp2.zhouji as week_day
            , (case when sum(tmp3.amt) = 0 then null else sum(tmp2.amt) / sum(tmp3.amt) end) as weight
            , year({p_year_start}) as ref_year_id
            , current_timestamp as etl_time
        from tmp2
        inner join tmp3 
            on tmp2.store_code = tmp3.store_code
        inner join edw.dim_stockorg a 
            on tmp2.store_code = a.org_code
        inner join edw.dim_stockorg b 
            on a.parent_id = b.org_id
        inner join edw.dim_stockorg c 
            on b.parent_id = c.org_id
        group by 
            c.org_longcode,tmp2.zhouji
    """.format(**{"p_year_start": p_year_start})

    spark.sql(sql)

    sql = """
        --门店维度
        insert into edw.dim_org_week_day_sales_weight
        select 
            org.org_id
            , tmp4.store_code as org_code
            , org.org_longcode as org_long_code
            , '门店' as org_type
            , tmp4.zhouji as week_day
            , tmp4.quanzhong as weight
            , year({p_year_start}) as ref_year_id
            , current_timestamp as etl_time
        from tmp4
        inner join edw.dim_stockorg as org
            on tmp4.store_code = org.org_code
    """.format(**{"p_year_start": p_year_start})

    spark.sql(sql)
