import os
from utils.tools import SparkInit
from datetime import datetime, date, timedelta
import time


def insert_day(day, index, tmp4, tmp1, tmp0):
    tmp6_sql = """
    select 
    t5.store_code,
    t7.temp_3*t5.per_3_turnover+t7.temp_4*t5.per_4_turnover+t7.temp_5*t5.per_5_turnover+t7.temp_6*t5.per_6_turnover+t7.temp_7*t5.per_7_turnover+t7.temp_8*t5.per_8_turnover+t7.temp_9*t5.per_9_turnover+t7.temp_10*t5.per_10_turnover+t7.temp_11*t5.per_11_turnover+t7.temp_12*t5.per_12_turnover+t7.temp_13*t5.per_13_turnover+t7.temp_14*t5.per_14_turnover+t7.temp_15*t5.per_15_turnover+t7.temp_16*t5.per_16_turnover+t7.temp_17*t5.per_17_turnover+t7.temp_18*t5.per_18_turnover+t7.temp_19*t5.per_19_turnover+t7.temp_20*t5.per_20_turnover+t7.temp_21*t5.per_21_turnover+t7.temp_22*t5.per_22_turnover+t7.temp_23*t5.per_23_turnover+t7.temp_0*t5.per_0_turnover+t7.temp_1*t5.per_1_turnover+t7.temp_2*t5.per_2_turnover temp_turnover,
    t7.pre_3*t5.per_3_turnover+t7.pre_4*t5.per_4_turnover+t7.pre_5*t5.per_5_turnover+t7.pre_6*t5.per_6_turnover+t7.pre_7*t5.per_7_turnover+t7.pre_8*t5.per_8_turnover+t7.pre_9*t5.per_9_turnover+t7.pre_10*t5.per_10_turnover+t7.pre_11*t5.per_11_turnover+t7.pre_12*t5.per_12_turnover+t7.pre_13*t5.per_13_turnover+t7.pre_14*t5.per_14_turnover+t7.pre_15*t5.per_15_turnover+t7.pre_16*t5.per_16_turnover+t7.pre_17*t5.per_17_turnover+t7.pre_18*t5.per_18_turnover+t7.pre_19*t5.per_19_turnover+t7.pre_20*t5.per_20_turnover+t7.pre_21*t5.per_21_turnover+t7.pre_22*t5.per_22_turnover+t7.pre_23*t5.per_23_turnover+t7.pre_0*t5.per_0_turnover+t7.pre_1*t5.per_1_turnover+t7.pre_2*t5.per_2_turnover pre_turnover
    from {1} t5
    left join rbu_sxcp_edw_dev.dim_store t6 on t5.store_code=t6.store_code 
    left join rbu_sxcp_ods_dev.rst_weather t7 
        on t6.city=t7.city 
        and t7.weather_date='{0}'
    """.format(day, tmp4)
    spark.create_temp_table(tmp6_sql, "tmp6")
    spark.explicit_cache_table("tmp6")
    if index == 0:
        sql_tmp7 = """
        select 
        row_number() over(partition by store_code,date_case order by sale_date desc) date_id,
        store_code,
        sale_date,
        date_case,
        turnover
        from rbu_sxcp_edw_ai_dev.turnover_feat
        where sale_date<'{0}' and turnover is not null
        """.format(day)
    else:
        sql_tmp7a = """
        select 
        store_code,
        sale_date,
        date_case,
        turnover
        from rbu_sxcp_edw_ai_dev.turnover_feat
        where sale_date<'{0}' and turnover is not null
        union all
        select
        store_code,
        sale_date,
        date_case,
        turnover
        from {1}
        """.format(day, tmp1)
        spark.create_temp_table(sql_tmp7a, "tmp7a")
        sql_tmp7 = """
        select 
        row_number() over(partition by store_code,date_case order by sale_date desc) date_id,
        store_code,
        sale_date,
        date_case,
        turnover
        from tmp7a
        """
    spark.create_temp_table(sql_tmp7, "tmp7")
    spark.explicit_cache_table("tmp7")
    tmp8_sql = """
    select 
    store_code,
    sale_date,
    date_case,
    turnover
    from tmp7
    where date_id<=30
    """
    spark.create_temp_table(tmp8_sql, "tmp8")
    spark.explicit_cache_table("tmp8")
    spark.explicit_uncache_table("tmp7")

    sql_tmp9a = """
    select 
    percentile_approx(turnover,0.25,9999)-3*(percentile_approx(turnover,0.75,9999)-percentile_approx(turnover,0.25,9999)) low,
    percentile_approx(turnover,0.75,9999)+3*(percentile_approx(turnover,0.75,9999)-percentile_approx(turnover,0.25,9999)) high,
    store_code,
    date_case
    from tmp8
    group by store_code,date_case
    """
    spark.create_temp_table(sql_tmp9a, "tmp9a")
    spark.explicit_cache_table("tmp9a")

    sql_tmp9 = """
    select 
    d.store_code,
    d.sale_date,
    d.date_case,
    d.turnover
    from tmp8 d 
    left join tmp9a f 
        on d.store_code=f.store_code 
        and d.date_case=f.date_case
    where d.turnover>=f.low 
        and d.turnover<=f.high
    """
    spark.create_temp_table(sql_tmp9, "tmp9")
    spark.explicit_cache_table("tmp9")
    spark.explicit_uncache_table("tmp8")

    sql_tmp10a = """
    select 
    row_number() over(partition by store_code,date_case order by sale_date desc) date_id,
    store_code,
    sale_date,
    date_case,
    turnover
    from tmp9
    where sale_date<'{0}'
    """.format(item)
    spark.create_temp_table(sql_tmp10a, "tmp10a")
    spark.explicit_cache_table("tmp10a")
    spark.explicit_uncache_table("tmp9")

    sql_tmp10 = """
    select 
    store_code,
    date_case,
    avg(turnover) recent_3
    from tmp10a
    where date_id<=3
    group by store_code,date_case
    """
    spark.create_temp_table(sql_tmp10, "tmp10")
    spark.explicit_cache_table("tmp10")

    sql_tmp11 = """
    select 
    store_code,
    date_case,
    avg(turnover) recent_7
    from tmp10a
    where date_id<=7
    group by store_code,date_case
    """
    spark.create_temp_table(sql_tmp11, "tmp11")
    spark.explicit_cache_table("tmp11")
    spark.explicit_uncache_table("tmp10a")

    sql_tmp12 = """
    select 
    t13.store_code,
    '{0}' sale_date,
    t14.case date_case
    from {1} t13
    left join rbu_sxcp_edw_ai_dev.store_category t14 
        on t13.store_code=t14.store_code 
        and t14.date='{0}'
    where t14.case is not null
        and t13.store_code is not null
    """.format(item, tmp0)
    spark.create_temp_table(sql_tmp12, "tmp12")
    spark.explicit_cache_table("tmp12")

    sql_tmp13 = """
    select 
    t8.store_code,
    t8.sale_date,
    t8.date_case,
    t12.turnover,
    t9.pre_turnover pre,
    t9.temp_turnover temp,
    t10.recent_3,
    t11.recent_7
    from tmp12 t8
    left join tmp6 t9 
        on t8.store_code=t9.store_code 
    left join tmp10 t10 
        on t8.store_code=t10.store_code
        and t8.date_case=t10.date_case 
    left join tmp11 t11 
        on t8.store_code=t11.store_code 
        and t8.date_case=t11.date_case 
    left join {1} t12 
        on t8.store_code=t12.store_code 
        and t8.sale_date=t12.sale_date
    where t8.sale_date='{0}' 
        and  t10.recent_3>=0 
        and t11.recent_7>=0
    """.format(item, tmp1)
    spark.create_temp_table(sql_tmp13, "tmp13")
    spark.explicit_cache_table("tmp13")
    spark.explicit_uncache_table("tmp6")
    spark.explicit_uncache_table("tmp10")
    spark.explicit_uncache_table("tmp11")
    spark.explicit_uncache_table("tmp12")

    print("开始插入{0}数据".format(day))
    sql_insert = """
    insert into table rbu_sxcp_edw_ai_dev.turnover_feat partition(year_month)
    (select 
    store_code,
    sale_date,
    date_case,
    turnover,
    pre,
    temp,
    recent_3,
    recent_7,
    current_timestamp() etl_time,
    substring(sale_date,1,7) year_month
    from tmp39)
    """
    start = time.time()
    spark.execute_sql(sql_insert)
    end = time.time()
    print('Running time: {0} Seconds'.format(end - start))

if __name__ == '__main__':
    # 获取文件的名字
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    # 得到昨天的日期
    item = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
    print("starting"+'.'*50)
    print("开始生成{0} {1}".format(item, file_name))

    sql_tmp0 = """
    select distinct store_code 
    from rbu_sxcp_edw_dev.fct_on_sale_history  
    where sale_date='{0}' 
        and store_code in ('st_code_0585', 'st_code_0292', 'st_code_0659') 
    """.format(item)
    spark.create_temp_table(sql_tmp0, "tmp0")

    sql_tmp1 = """
    select t8.store_code,
    t8.date sale_date,
    t8.case date_case,
    sum(t10.sale_total_qty*COALESCE(t11.sale_price,0)) turnover
    from rbu_sxcp_edw_ai_dev.store_category t8
    inner join tmp0 t9
        on t8.store_code=t9.store_code
    left join rbu_sxcp_edw_ai_dev.store_sale_add t10 on t8.store_code=t10.store_code and t8.date=t10.check_date
    left join rbu_sxcp_edw_dev.dim_store_product t11 on t10.store_code=t11.store_code and t10.product_code=t11.product_code
    where t8.date='{0}'
    group by t8.store_code,t8.date,t8.case
    """.format(item)
    spark.create_temp_table(sql_tmp1, "tmp1")
    spark.explicit_cache_table("tmp1")

    print("取{0}一年的值,求各个时间点的四分位数".format(item))
    sql_tmp2a = """
    select 
    percentile_approx(time_3_turnover,0.25,9999)-3*(percentile_approx(time_3_turnover,0.75,9999)-percentile_approx(time_3_turnover,0.25,9999)) low_3,
    percentile_approx(time_3_turnover,0.75,9999)+3*(percentile_approx(time_3_turnover,0.75,9999)-percentile_approx(time_3_turnover,0.25,9999)) high_3,
    percentile_approx(time_4_turnover,0.25,9999)-3*(percentile_approx(time_4_turnover,0.75,9999)-percentile_approx(time_4_turnover,0.25,9999)) low_4,
    percentile_approx(time_4_turnover,0.75,9999)+3*(percentile_approx(time_4_turnover,0.75,9999)-percentile_approx(time_4_turnover,0.25,9999)) high_4,
    percentile_approx(time_5_turnover,0.25,9999)-3*(percentile_approx(time_5_turnover,0.75,9999)-percentile_approx(time_5_turnover,0.25,9999)) low_5,
    percentile_approx(time_5_turnover,0.75,9999)+3*(percentile_approx(time_5_turnover,0.75,9999)-percentile_approx(time_5_turnover,0.25,9999)) high_5,
    percentile_approx(time_6_turnover,0.25,9999)-3*(percentile_approx(time_6_turnover,0.75,9999)-percentile_approx(time_6_turnover,0.25,9999)) low_6,
    percentile_approx(time_6_turnover,0.75,9999)+3*(percentile_approx(time_6_turnover,0.75,9999)-percentile_approx(time_6_turnover,0.25,9999)) high_6,
    percentile_approx(time_7_turnover,0.25,9999)-3*(percentile_approx(time_7_turnover,0.75,9999)-percentile_approx(time_7_turnover,0.25,9999)) low_7,
    percentile_approx(time_7_turnover,0.75,9999)+3*(percentile_approx(time_7_turnover,0.75,9999)-percentile_approx(time_7_turnover,0.25,9999)) high_7,
    percentile_approx(time_8_turnover,0.25,9999)-3*(percentile_approx(time_8_turnover,0.75,9999)-percentile_approx(time_8_turnover,0.25,9999)) low_8,
    percentile_approx(time_8_turnover,0.75,9999)+3*(percentile_approx(time_8_turnover,0.75,9999)-percentile_approx(time_8_turnover,0.25,9999)) high_8,
    percentile_approx(time_9_turnover,0.25,9999)-3*(percentile_approx(time_9_turnover,0.75,9999)-percentile_approx(time_9_turnover,0.25,9999)) low_9,
    percentile_approx(time_9_turnover,0.75,9999)+3*(percentile_approx(time_9_turnover,0.75,9999)-percentile_approx(time_9_turnover,0.25,9999)) high_9,
    percentile_approx(time_10_turnover,0.25,9999)-3*(percentile_approx(time_10_turnover,0.75,9999)-percentile_approx(time_10_turnover,0.25,9999)) low_10,
    percentile_approx(time_10_turnover,0.75,9999)+3*(percentile_approx(time_10_turnover,0.75,9999)-percentile_approx(time_10_turnover,0.25,9999)) high_10,
    percentile_approx(time_11_turnover,0.25,9999)-3*(percentile_approx(time_11_turnover,0.75,9999)-percentile_approx(time_11_turnover,0.25,9999)) low_11,
    percentile_approx(time_11_turnover,0.75,9999)+3*(percentile_approx(time_11_turnover,0.75,9999)-percentile_approx(time_11_turnover,0.25,9999)) high_11,
    percentile_approx(time_12_turnover,0.25,9999)-3*(percentile_approx(time_12_turnover,0.75,9999)-percentile_approx(time_12_turnover,0.25,9999)) low_12,
    percentile_approx(time_12_turnover,0.75,9999)+3*(percentile_approx(time_12_turnover,0.75,9999)-percentile_approx(time_12_turnover,0.25,9999)) high_12,
    percentile_approx(time_13_turnover,0.25,9999)-3*(percentile_approx(time_13_turnover,0.75,9999)-percentile_approx(time_13_turnover,0.25,9999)) low_13,
    percentile_approx(time_13_turnover,0.75,9999)+3*(percentile_approx(time_13_turnover,0.75,9999)-percentile_approx(time_13_turnover,0.25,9999)) high_13,
    percentile_approx(time_14_turnover,0.25,9999)-3*(percentile_approx(time_14_turnover,0.75,9999)-percentile_approx(time_14_turnover,0.25,9999)) low_14,
    percentile_approx(time_14_turnover,0.75,9999)+3*(percentile_approx(time_14_turnover,0.75,9999)-percentile_approx(time_14_turnover,0.25,9999)) high_14,
    percentile_approx(time_15_turnover,0.25,9999)-3*(percentile_approx(time_15_turnover,0.75,9999)-percentile_approx(time_15_turnover,0.25,9999)) low_15,
    percentile_approx(time_15_turnover,0.75,9999)+3*(percentile_approx(time_15_turnover,0.75,9999)-percentile_approx(time_15_turnover,0.25,9999)) high_15,
    percentile_approx(time_16_turnover,0.25,9999)-3*(percentile_approx(time_16_turnover,0.75,9999)-percentile_approx(time_16_turnover,0.25,9999)) low_16,
    percentile_approx(time_16_turnover,0.75,9999)+3*(percentile_approx(time_16_turnover,0.75,9999)-percentile_approx(time_16_turnover,0.25,9999)) high_16,
    percentile_approx(time_17_turnover,0.25,9999)-3*(percentile_approx(time_17_turnover,0.75,9999)-percentile_approx(time_17_turnover,0.25,9999)) low_17,
    percentile_approx(time_17_turnover,0.75,9999)+3*(percentile_approx(time_17_turnover,0.75,9999)-percentile_approx(time_17_turnover,0.25,9999)) high_17,
    percentile_approx(time_18_turnover,0.25,9999)-3*(percentile_approx(time_18_turnover,0.75,9999)-percentile_approx(time_18_turnover,0.25,9999)) low_18,
    percentile_approx(time_18_turnover,0.75,9999)+3*(percentile_approx(time_18_turnover,0.75,9999)-percentile_approx(time_18_turnover,0.25,9999)) high_18,
    percentile_approx(time_19_turnover,0.25,9999)-3*(percentile_approx(time_19_turnover,0.75,9999)-percentile_approx(time_19_turnover,0.25,9999)) low_19,
    percentile_approx(time_19_turnover,0.75,9999)+3*(percentile_approx(time_19_turnover,0.75,9999)-percentile_approx(time_19_turnover,0.25,9999)) high_19,
    percentile_approx(time_20_turnover,0.25,9999)-3*(percentile_approx(time_20_turnover,0.75,9999)-percentile_approx(time_20_turnover,0.25,9999)) low_20,
    percentile_approx(time_20_turnover,0.75,9999)+3*(percentile_approx(time_20_turnover,0.75,9999)-percentile_approx(time_20_turnover,0.25,9999)) high_20,
    percentile_approx(time_21_turnover,0.25,9999)-3*(percentile_approx(time_21_turnover,0.75,9999)-percentile_approx(time_21_turnover,0.25,9999)) low_21,
    percentile_approx(time_21_turnover,0.75,9999)+3*(percentile_approx(time_21_turnover,0.75,9999)-percentile_approx(time_21_turnover,0.25,9999)) high_21,
    percentile_approx(time_22_turnover,0.25,9999)-3*(percentile_approx(time_22_turnover,0.75,9999)-percentile_approx(time_22_turnover,0.25,9999)) low_22,
    percentile_approx(time_22_turnover,0.75,9999)+3*(percentile_approx(time_22_turnover,0.75,9999)-percentile_approx(time_22_turnover,0.25,9999)) high_22,
    percentile_approx(time_23_turnover,0.25,9999)-3*(percentile_approx(time_23_turnover,0.75,9999)-percentile_approx(time_23_turnover,0.25,9999)) low_23,
    percentile_approx(time_23_turnover,0.75,9999)+3*(percentile_approx(time_23_turnover,0.75,9999)-percentile_approx(time_23_turnover,0.25,9999)) high_23,
    percentile_approx(time_0_turnover,0.25,9999)-3*(percentile_approx(time_0_turnover,0.75,9999)-percentile_approx(time_0_turnover,0.25,9999)) low_24,
    percentile_approx(time_0_turnover,0.75,9999)+3*(percentile_approx(time_0_turnover,0.75,9999)-percentile_approx(time_0_turnover,0.25,9999)) high_24,
    percentile_approx(time_1_turnover,0.25,9999)-3*(percentile_approx(time_1_turnover,0.75,9999)-percentile_approx(time_1_turnover,0.25,9999)) low_25,
    percentile_approx(time_1_turnover,0.75,9999)+3*(percentile_approx(time_1_turnover,0.75,9999)-percentile_approx(time_1_turnover,0.25,9999)) high_25,
    percentile_approx(time_2_turnover,0.25,9999)-3*(percentile_approx(time_2_turnover,0.75,9999)-percentile_approx(time_2_turnover,0.25,9999)) low_26,
    percentile_approx(time_2_turnover,0.75,9999)+3*(percentile_approx(time_2_turnover,0.75,9999)-percentile_approx(time_2_turnover,0.25,9999)) high_26,
    store_code
    from rbu_sxcp_edw_ai_dev.turnover_feat_sum
    where sale_date>=date_sub('{0}',365) and sale_date<='{0}'
    group by store_code
    """.format(item)
    spark.create_temp_table(sql_tmp2a, "tmp2a")

    print("剔除一年内数据的异常值")
    sql_tmp2 = """
    select 
    t4.store_code,
    t4.sale_date,
    case when t4.time_3_turnover>=low_3 and t4.time_3_turnover<=high_3 then t4.time_3_turnover
        else null 
    end time_3_turnover,
    case when t4.time_4_turnover>=low_4 and t4.time_4_turnover<=high_4 then t4.time_4_turnover
        else null 
    end time_4_turnover,
    case when t4.time_5_turnover>=low_5 and t4.time_5_turnover<=high_5 then t4.time_5_turnover
        else null 
    end time_5_turnover,
    case when t4.time_6_turnover>=low_6 and t4.time_6_turnover<=high_6 then t4.time_6_turnover
        else null 
    end time_6_turnover,
    case when t4.time_7_turnover>=low_7 and t4.time_7_turnover<=high_7 then t4.time_7_turnover
        else null 
    end time_7_turnover,
    case when t4.time_8_turnover>=low_8 and t4.time_8_turnover<=high_8 then t4.time_8_turnover
        else null 
    end time_8_turnover,
    case when t4.time_9_turnover>=low_9 and t4.time_9_turnover<=high_9 then t4.time_9_turnover
        else null 
    end time_9_turnover,
    case when t4.time_10_turnover>=low_10 and t4.time_10_turnover<=high_10 then t4.time_10_turnover
        else null 
    end time_10_turnover,
    case when t4.time_11_turnover>=low_11 and t4.time_11_turnover<=high_11 then t4.time_11_turnover
        else null 
    end time_11_turnover,
    case when t4.time_12_turnover>=low_12 and t4.time_12_turnover<=high_12 then t4.time_12_turnover
        else null 
    end time_12_turnover,
    case when t4.time_13_turnover>=low_13 and t4.time_13_turnover<=high_13 then t4.time_13_turnover
        else null 
    end time_13_turnover,
    case when t4.time_14_turnover>=low_14 and t4.time_14_turnover<=high_14 then t4.time_14_turnover
        else null 
    end time_14_turnover,
    case when t4.time_15_turnover>=low_15 and t4.time_15_turnover<=high_15 then t4.time_15_turnover
        else null 
    end time_15_turnover,
    case when t4.time_16_turnover>=low_16 and t4.time_16_turnover<=high_16 then t4.time_16_turnover
        else null 
    end time_16_turnover,
    case when t4.time_17_turnover>=low_17 and t4.time_17_turnover<=high_17 then t4.time_17_turnover
        else null 
    end time_17_turnover,
    case when t4.time_18_turnover>=low_18 and t4.time_18_turnover<=high_18 then t4.time_18_turnover
        else null 
    end time_18_turnover,
    case when t4.time_19_turnover>=low_19 and t4.time_19_turnover<=high_19 then t4.time_19_turnover
        else null 
    end time_19_turnover,
    case when t4.time_20_turnover>=low_20 and t4.time_20_turnover<=high_20 then t4.time_20_turnover
        else null 
    end time_20_turnover,
    case when t4.time_21_turnover>=low_21 and t4.time_21_turnover<=high_21 then t4.time_21_turnover
        else null 
    end time_21_turnover,
    case when t4.time_22_turnover>=low_22 and t4.time_22_turnover<=high_22 then t4.time_22_turnover
        else null 
    end time_22_turnover,
    case when t4.time_23_turnover>=low_23 and t4.time_23_turnover<=high_23 then t4.time_23_turnover
        else null 
    end time_23_turnover,
    case when t4.time_0_turnover>=low_24 and t4.time_0_turnover<=high_24 then t4.time_0_turnover
        else null 
    end time_0_turnover,
    case when t4.time_1_turnover>=low_25 and t4.time_1_turnover<=high_25 then t4.time_1_turnover
        else null 
    end time_1_turnover,
    case when t4.time_2_turnover>=low_26 and t4.time_2_turnover<=high_26 then t4.time_2_turnover
        else null 
    end time_2_turnover
    from rbu_sxcp_edw_ai_dev.turnover_feat_sum t4
    left join tmp2a t5 on t4.store_code=t5.store_code
    where t4.sale_date>=date_sub('{0}',365) and t4.sale_date<='{0}'
    """.format(item)
    spark.create_temp_table(sql_tmp2, "tmp2")

    sql_tmp3 = """
    select 
    store_code,
    avg(time_3_turnover) avg_3_turnover,
    avg(time_4_turnover) avg_4_turnover,
    avg(time_5_turnover) avg_5_turnover,
    avg(time_6_turnover) avg_6_turnover,
    avg(time_7_turnover) avg_7_turnover,
    avg(time_8_turnover) avg_8_turnover,
    avg(time_9_turnover) avg_9_turnover,
    avg(time_10_turnover) avg_10_turnover,
    avg(time_11_turnover) avg_11_turnover,
    avg(time_12_turnover) avg_12_turnover,
    avg(time_13_turnover) avg_13_turnover,
    avg(time_14_turnover) avg_14_turnover,
    avg(time_15_turnover) avg_15_turnover,
    avg(time_16_turnover) avg_16_turnover,
    avg(time_17_turnover) avg_17_turnover,
    avg(time_18_turnover) avg_18_turnover,
    avg(time_19_turnover) avg_19_turnover,
    avg(time_20_turnover) avg_20_turnover,
    avg(time_21_turnover) avg_21_turnover,
    avg(time_22_turnover) avg_22_turnover,
    avg(time_23_turnover) avg_23_turnover,
    avg(time_0_turnover) avg_0_turnover,
    avg(time_1_turnover) avg_1_turnover,
    avg(time_2_turnover) avg_2_turnover,
    case when avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover)=0 then 1 
        else avg(time_3_turnover)+avg(time_4_turnover)+avg(time_5_turnover)+avg(time_6_turnover)+avg(time_7_turnover)+avg(time_8_turnover)+avg(time_9_turnover)+avg(time_10_turnover)+avg(time_11_turnover)+avg(time_12_turnover)+avg(time_13_turnover)+avg(time_14_turnover)+avg(time_15_turnover)+avg(time_16_turnover)+avg(time_17_turnover)+avg(time_18_turnover)+avg(time_19_turnover)+avg(time_20_turnover)+avg(time_21_turnover)+avg(time_22_turnover)+avg(time_23_turnover)+avg(time_0_turnover)+avg(time_1_turnover)+avg(time_2_turnover) 
    end sum_avg
    from tmp2
    group by store_code
    """
    spark.create_temp_table(sql_tmp3, "tmp3")
    spark.explicit_cache_table("tmp3")
    spark.drop_temp_table("tmp2")

    sql_tmp4 = """
    select 
    store_code,
    avg_3_turnover/sum_avg per_3_turnover,
    avg_4_turnover/sum_avg per_4_turnover,
    avg_5_turnover/sum_avg per_5_turnover,
    avg_6_turnover/sum_avg per_6_turnover,
    avg_7_turnover/sum_avg per_7_turnover,
    avg_8_turnover/sum_avg per_8_turnover,
    avg_9_turnover/sum_avg per_9_turnover,
    avg_10_turnover/sum_avg per_10_turnover,
    avg_11_turnover/sum_avg per_11_turnover,
    avg_12_turnover/sum_avg per_12_turnover,
    avg_13_turnover/sum_avg per_13_turnover,
    avg_14_turnover/sum_avg per_14_turnover,
    avg_15_turnover/sum_avg per_15_turnover,
    avg_16_turnover/sum_avg per_16_turnover,
    avg_17_turnover/sum_avg per_17_turnover,
    avg_18_turnover/sum_avg per_18_turnover,
    avg_19_turnover/sum_avg per_19_turnover,
    avg_20_turnover/sum_avg per_20_turnover,
    avg_21_turnover/sum_avg per_21_turnover,
    avg_22_turnover/sum_avg per_22_turnover,
    avg_23_turnover/sum_avg per_23_turnover,
    avg_0_turnover/sum_avg per_0_turnover,
    avg_1_turnover/sum_avg per_1_turnover,
    avg_2_turnover/sum_avg per_2_turnover
    from tmp3
    """
    spark.create_temp_table(sql_tmp4, "tmp4")
    spark.explicit_cache_table("tmp4")

    print("回写历史数据")
    sql_tmp5 = """
    insert overwrite table rbu_sxcp_edw_ai_dev.turnover_feat partition(year_month)
    (select * 
    from rbu_sxcp_edw_ai_dev.turnover_feat 
    where sale_date<'{0}' 
        and sale_date>=concat(substring('{0}',1,7),'-01'))
    """.format(item)
    spark.execute_sql(sql_tmp5)

    # 开始循环插入昨天和未来七天的数据
    for i in range(0, 8):
        day = datetime.strptime(item, "%Y-%m-%d") + timedelta(days=i)
        print("{0}营业额特征数据的代码".format(str(day)[:10]))
        insert_day(str(day)[:10], i, "tmp4", "tmp1", "tmp0")

    print("process successfully!")
    print('.'*50)





