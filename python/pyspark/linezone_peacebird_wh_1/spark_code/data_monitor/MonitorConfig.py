# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-wh-1
# Filename: config
# Author: zsm
# Date: 2018/10/30 19:44
# ----------------------------------------------------------------------------


# 总配置
class MonitorConfig(object):

    # 项目名称+任务
    PROJECT = 'PeacebirdDataBase'

    # 几个需要做数据量校验的origin schema
    # 当真实schema名改为dms/dm_test, rsts/rst_test等时不用更改
    # 当新增了需要做数据量监控的schema时需要加在列表中
    TABLE_PREFIX = ['edw', 'dm', 'rst']

    # 现有数据库中需要检查某天数据的表的日期表示
    DAY_DATE_LIST = ['day_date',
                     'stock_date',
                     'sale_date',
                     'pre_day_date',
                     'dec_day_date',
                     'assess_date',
                     'cla_week_date',
                     ]

    ETL_TIME = ['etl_time',
                'update_time',
                ]

    # 现有按周表示的表名
    WEEK_MON_TABLE_LIST = ['mod_skc_week_classify',
                           'dm_sku_org_week_replenish_allot_return_comp',
                           'rst_skc_city_week_suitability',
                           ]
    # 需要检查输入日期加一天的表
    ADD_ONE_DAY_TABLE_LIST = ['mod_sku_store_day_target_inventory',
                              'mod_sku_day_replenish',
                              'rst_sku_replenish_day_drp',
                              'rst_sku_allot_day_drp',
                              'rst_sku_store_target_inventory_day_drp',
                              ]

    # 不放到WEEK_MON_TABLE_LIST是因为需要检测当前日期+1天
    # 不放到ADD_ONE_DAY_TABLE_LIST是因为每周跑一次的数据有时候没有是正常的，
    # 但放在ADD_ONE_DAY_TABLE_LIST会发邮件提醒此表没有当天所需数据
    # 检查加一天所在周一的数据的表
    ADD_ONE_DAY_MON_TABLE_LIST = ['mod_sku_day_allot',
                                  'mod_sku_day_return_result',
                                  ]

    # 检查当前日期减6天所在周一的数据
    SUB_SIX_DAY_MON_TABLE_LIST = ['rst_skc_city_week_sales_change_comp',
                                  ]
    MAIL_HOST = 'smtp.163.com'

    MAIL_PORT = 25

    # 发件邮箱账号
    MAIL_USER = 'zsm_data_monitor'

    # 注意不是真正的passwd，而是发件邮箱中设置的SMTP授权码
    MAIL_PASSWD = 'lanai123'

    # 发件邮箱地址
    SENDER = 'zsm_data_monitor@163.com'

    # 发件人别名
    SENDER_NAME = 'zsm_data_monitor'

    # 注意一定要将发件邮箱地址添加到收件邮箱地址，
    # 否则多次发送后会被认为发送内容不合法bulabula！！！
    RECEIVERS = ['zsm_data_monitor@163.com',
                 'liuhai@linezonedata.com',
                 'zhangsongmiao@linezonedata.com',
                 '1293043519@qq.com']

    # 邮件主题
    SUBJECT = 'Warning of {Monitor_class}-{project}'

    SHOW_TABLE_NAMES_SQL = """
        show tables from {schema}
        """

    SHOW_TABLE_COLUMNS_SQL = """
        desc {schema}.{table_name}
        """


# 统计各层schema表的数据量的配置
class DataCountConfig(MonitorConfig):

    # data_count临时表名称
    TMP = 'data_count_tmp'

    # 目标插入表
    TARGET_TABLE = '_table_day_data_count'

    DATA_COUNT_SQL = """
        select 
            '{schema}.{table_name}' as table_name
            , count(1) as data_count
        from {schema}.{table_name} 
        {where_condi}
        """
    INSERT_SQL = """
        insert overwrite table {schema}.{table_prefix}{target_table}
        partition(day_date)
        select 
            {tmp}.table_name
            , {tmp}.data_count
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {tmp}
        """
    # data_count数据总量监控的邮件内容
    DATA_COUNT_EMAIL_CONT = 'The following tables have no data ' + \
        'on {p_input_date}:\n{empty_table_str}'


# 统计模型表是否存在重复数据的配置
class DataDupConfig(MonitorConfig):

    # data_duplication 临时表名称
    TMP = 'model_dup_tmp'

    # 目标插入表
    TARGET_TABLE = 'mod_table_day_data_duplication'

    # 如果这天表*没有数据，由于加了group by因此会导致没有表*是否重复的统计数据
    # 这也是合理的，当统计数据总量时会统计到这部分表，且没有数据的表本身就不应该标记为没有重复数据
    DATA_DUP_SQL = """
        with t as (
            select 
                count(1) as counter
            from {schema}.{table_name} 
            {where_condi}
            group by `{group_by_columns}`
            having counter > 1
            )
        select
            '{schema}.{table_name}' as table_name
            , (case when count(1)>0 then 'Y' else 'N' end) as is_dup
        from t
        """
    INSERT_SQL = """
        insert overwrite table {schema}.{target_table}
        partition(day_date)
        select
            {tmp}.table_name
            , {tmp}.is_dup
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {tmp}
    """
    # data_duplication数据重复监控的邮件内容
    DATA_DUP_EMAIL_CONT = 'The following tables including modeling data have duplication ' + \
        'on {p_input_date}:\n{dup_table_str}'

