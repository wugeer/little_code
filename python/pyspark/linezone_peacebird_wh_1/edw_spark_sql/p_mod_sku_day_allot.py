# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird
# Filename: p_mod_sku_day_allot
# Author: yangzhitao
# Date: 2019/01/10 09:27
# ----------------------------------------------------------------------------
import sys
import time
import datetime
from datetime import timedelta
import os
from dateutil.parser import parse
from multiprocessing import cpu_count
from concurrent import futures
import pandas as pd
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class SparkInit(object):
    def __init__(self, file_name):
        warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')
        # app的名称
        app_name = "".join(["PySpark-", file_name])

        # config 配置
        spark_conf = SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        spark_conf.set("hive.exec.dynamic.partition.mode", 'nonstrict')
        # 目的是解决报错：Detected cartesian product for INNER join between logical plans
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')

        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()

        # 获取脚本执行需要的参数
        self.params_dict = self.get_params()

    @staticmethod
    def get_params():
        """
        获取参数, 返回python脚本的参数字典,
        :return: params_dict 默认返回输入日期和输入日期截断到周一的日期, 正价率的字段，畅销款比率
        """
        param_list = sys.argv

        p_input_date = parse(param_list[1]).strftime('%Y-%m-%d')
        p_input_date_mon = date_trunc('week', p_input_date)
        p_input_date_add_one_day = (parse(p_input_date) + timedelta(days=1)).strftime('%Y-%m-%d')

        params_dict = {'p_input_date': p_input_date,
                       'p_input_date_add_one_day': p_input_date_add_one_day,
                       'p_input_date_mon': p_input_date_mon
                       }
        return params_dict

    def create_temp_table(self, sql, table_name):
        """
        创建临时表
        :param sql: sql语句
        :param table_name: 临时表表名
        :return:
        """
        sql_temp = sql.format(**self.params_dict)
        temp_table = self.spark.sql(sql_temp).createOrReplaceTempView(table_name)
        return temp_table

    def drop_temp_table(self, table_name):
        """
        drop临时表
        :param table_name:
        :return:
        """
        self.spark.catalog.dropTempView(table_name)

    def execute_sql(self, sql):
        """
        spark引擎执行sql语句
        :param sql:
        :return:
        """
        sql_to_execute = sql.format(**self.params_dict)
        df = self.spark.sql(sql_to_execute)
        return df


def date_trunc(interval, date_str):
    """
    截断到指定精度，返回相应的日期字符串
    :param interval: ['week', 'month', 'year']
    :param date_str:
    :return: after_trunc_date_str
    """
    date_obj = parse(date_str)
    # date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == 'week':
        res = date_obj - timedelta(days=(date_obj.isocalendar()[2] - 1))
    elif interval == 'month':
        res = datetime.date(date_obj.year, date_obj.month, 1)
    elif interval == 'year':
        res = datetime.date(date_obj.year, 1, 1)
    else:
        raise Exception("interval must be ['week', 'month', 'year']")
    return res.strftime('%Y-%m-%d')


def main():

    """ == tmp_main_alter_sku ==
    主款          替代款
    main_skuid | main_alter_skuid
    sku1            sku1
    sku1            a1
    sku1            a2
    ---------------------------
    """
    # 主款、替代款sku关系
    sql_tmp_main_alter_sku = '''
            select 
                m_productalias_id as main_skuid 
                , m_productalias_id as main_alter_skuid 
                -- , '0' as priority
            from peacebird.st_sku_relate_view
            group by m_productalias_id
            union all 
            select 
                m_productalias_id as main_skuid 
                , m_productalias2_id as main_alter_skuid
                --, priority
            from peacebird.st_sku_relate_view
        '''
    spark.create_temp_table(sql_tmp_main_alter_sku, "tmp_main_alter_sku")

    # 调拨单涉及的所有门店中，主款/替代款的库存及其SKC库存
    # 主款sku_id      替代款sku_id     门店id    门店sku库存   门店skc库存
    # main_skuid | main_alter_skuid | org_id | stock_qty | skc_stock_qty |
    sql_main_alter_sku_skc_stock = '''
    with 
        tmp_warehouse_stock as 
            (select 
                org_id
                , product_id
                , color_id
                , size_id
                , stock_qty
            from edw.mid_day_end_stock as stock
            where stock_date = '{p_input_date}'
                and exists(
                    select 1 
                    from edw.mod_sku_day_allot_model as allot  
                    where allot.dec_day_date = '{p_input_date_add_one_day}'   -- 决策日期
                        and allot.send_store_id=stock.org_id
                )
            ),
        tmp_warehouse_skc_stock as 
            (select 
                org_id
                , product_id
                , color_id
                , sum(stock_qty) as skc_stock_qty
            from edw.mid_day_end_stock as stock
            group by org_id, product_id, color_id, stock_date
            having stock_date = '{p_input_date}'
                and exists(
                    select 1 
                    from edw.mod_sku_day_allot_model as allot 
                    where allot.dec_day_date = '{p_input_date_add_one_day}'   -- 决策日期
                        and allot.send_store_id=stock.org_id
                )
            )
        select 
            sku_stock.main_skuid
            , sku_stock.main_alter_skuid
            , sku_stock.org_id
            , sku_stock.stock_qty
            , skc_stock.skc_stock_qty
        from (
            select 
                tmp_main_alter_sku.main_skuid
                , tmp_main_alter_sku.main_alter_skuid
                -- , tmp_main_alter_sku.priority
                , stock.org_id
                , stock.product_id
                , stock.color_id
                , stock.stock_qty 
            from tmp_warehouse_stock as stock
            inner join edw.dim_product_sku as dim_sku
                on dim_sku.product_id = stock.product_id
                and dim_sku.color_id = stock.color_id 
                and dim_sku.size_id = stock.size_id
            inner join tmp_main_alter_sku 
                on tmp_main_alter_sku.main_alter_skuid = dim_sku.sku_id
        ) as sku_stock
        inner join (
            select 
                stock.org_id
                , stock.product_id
                , stock.color_id
                , stock.skc_stock_qty
            from tmp_warehouse_skc_stock as stock
            inner join edw.dim_product_skc as dim_skc
                on dim_skc.product_id = stock.product_id
                and dim_skc.color_id = stock.color_id 
        ) as skc_stock
            on sku_stock.org_id = skc_stock.org_id
            and sku_stock.product_id = skc_stock.product_id
            and sku_stock.color_id = skc_stock.color_id
    '''
    main_alter_sku_skc_stock = spark.execute_sql(sql_main_alter_sku_skc_stock)

    # 原始模型表
    # 不需要处理的模型数据(补货建议中sku_id不在主款、替代款库存表中的数据)
    sql_satis_model = """
        select
            sku.sku_id
            , a.send_store_id
            , a.receive_store_id
            , a.date_send
            , a.date_rec_pred
            , a.send_qty
            , a.dec_day_date
        from (
            select * 
            from edw.mod_sku_day_allot_model    -- sku调拨表(模型输出)
            where dec_day_date = '{p_input_date_add_one_day}'   -- 决策日期
        ) as a
        inner join edw.dim_product_sku as sku         
            on a.product_id = sku.product_id 
            and a.color_id = sku.color_id 
            and a.size_id = sku.size_id
        left join tmp_main_alter_sku as tmp_sku
            on sku.sku_id = tmp_sku.main_skuid
        where tmp_sku.main_skuid is null
    """
    satis_model = spark.execute_sql(sql_satis_model)
    print('不需要拆分的补货建议数：{}'.format(satis_model.count()))
    # 需要处理的模型数据（sku_id在主款、替代款库存表中的数据）
    sql_origin_model = """
            select
                sku.sku_id
                , a.send_store_id
                , a.receive_store_id
                , a.date_send
                , a.date_rec_pred
                , a.send_qty
                , a.dec_day_date
            from (
                select * 
                from edw.mod_sku_day_allot_model
                where dec_day_date = '{p_input_date_add_one_day}'
            ) as a
            inner join edw.dim_product_sku as sku         
                on a.product_id = sku.product_id 
                and a.color_id = sku.color_id 
                and a.size_id = sku.size_id
            inner join (
                select distinct main_skuid
                from tmp_main_alter_sku
            ) as tmp_sku
                on sku.sku_id = tmp_sku.main_skuid
        """
    origin_model = spark.execute_sql(sql_origin_model)
    print('需要拆分的补货建议数：{}'.format(origin_model.count()))
    unpack_model(main_alter_sku_skc_stock, satis_model, origin_model)


def unpack_model(main_alter_sku_skc_stock, satis_model, origin_model):
    main_alter_sku_skc_stock = main_alter_sku_skc_stock.toPandas()

    # 判断是否有需要拆分的模型数据,有则处理，无则直接写入mod_sku_day_allot
    if origin_model.count() != 0:
        origin_model = origin_model.toPandas()
        unpack_list = []
        # 需要拆分的每一条模型建议
        print("===start unpacking===")
        # print("all stocks records: {}".format(main_alter_sku_skc_stock.shape[0]))
        for i in range(origin_model.shape[0]):
            suggest_df = origin_model[i:i + 1]
            sku_id = suggest_df['sku_id'].values[0]
            send_qty_init = suggest_df['send_qty'].values[0]
            send_store_id = suggest_df['send_store_id'].values[0]   # 发货门店id
            send_qty = send_qty_init
            # 发货门店库存
            stock_order_df = main_alter_sku_skc_stock[
                (main_alter_sku_skc_stock['org_id'] == send_store_id)
                & (main_alter_sku_skc_stock['main_skuid'] == sku_id)] \
                .sort_values(by='skc_stock_qty')
            # print("# alter_sku stock records: {}".format(stock_order_df.shape[0]))
            # print("org_id: {} \n".format(stock_order_df["org_id"].iloc[0]))
            # for row in stock_order_df.itertuples():
            #     print((
            #         "--- \n"
            #         "main_skuid: {} \n"
            #         "main_alter_skuid: {} \n"
            #         "stock_qty: {} \n"
            #         "skc_stock_qty: {} \n"
            #     ).format(row.main_skuid,
            #              row.main_alter_skuid,
            #              row.stock_qty,
            #              row.skc_stock_qty,
            #              )
            #     )
            for j in range(stock_order_df.shape[0]):
                alter_stock_df = stock_order_df[j:j + 1]    # 当前第一顺位的替代款
                alter_stock_qty = alter_stock_df['stock_qty'].values[0]     # 替代款库存
                alter_skc_stock_qty = alter_stock_df['skc_stock_qty'].values[0]     # 替代款skc库存
                alter_sku_id = alter_stock_df['main_alter_skuid'].values[0]
                index = main_alter_sku_skc_stock[
                    (main_alter_sku_skc_stock['org_id'] == send_store_id)
                    & (main_alter_sku_skc_stock['main_alter_skuid'] == alter_sku_id)]. \
                    index.tolist()[0]
                # print("({}, {})\talter_stock_qty`{}` | send_qty`{}`".format(i, j, alter_stock_qty, send_qty))
                # print("\tsend_store_id `{}` | alter_sku_id`{}`".format(send_store_id, alter_sku_id))
                if alter_stock_qty >= send_qty:
                    # if j == 0:
                    #     unpack_list.append(suggest_df)
                    # else:
                    # 把新生成的一条放入原始模型df
                    extend_suggest_df = suggest_df.replace(sku_id, alter_sku_id, inplace=False) \
                        .replace(send_qty_init, send_qty)
                    unpack_list.append(extend_suggest_df)
                    # 相应的库存赋值为alter_stock_qty-send_qty，skc库存同理
                    main_alter_sku_skc_stock[index: index + 1]['stock_qty'] = alter_stock_qty - send_qty
                    main_alter_sku_skc_stock[index: index + 1]['skc_stock_qty'] = alter_skc_stock_qty - send_qty
                    # print(alter_sku_id, alter_stock_qty)
                    break
                # 如果不够，先取出当前这条所有数量，再减去此次得到的数量，得到还需要多少qty,继续下一次循环
                else:
                    if alter_stock_qty <= 0:
                        continue
                    else:
                        extend_suggest_df = suggest_df.replace(sku_id, alter_sku_id, inplace=False) \
                            .replace(send_qty_init, alter_stock_qty)
                        unpack_list.append(extend_suggest_df)
                        # 相应替代款的库存赋值为0，替代款的skc库存减去该sku的库存
                        main_alter_sku_skc_stock[index: index + 1]['stock_qty'] = 0
                        main_alter_sku_skc_stock[index: index + 1]['skc_stock_qty'] = alter_skc_stock_qty - alter_stock_qty
                        send_qty = send_qty - alter_stock_qty   # 剩余补调量
        print("===end unpacking===")

        # 把所有unpack_list中的df合并为大的df
        if len(unpack_list) != 0:
            unpack_model_df = pd.concat(unpack_list, axis=0)
            unpack_model_spark_df = spark.spark.createDataFrame(unpack_model_df)
            all_unpack_model_spark_df = unpack_model_spark_df.unionAll(satis_model)
        else:
            all_unpack_model_spark_df = satis_model
    else:
        print("===no unpacking===")
        all_unpack_model_spark_df = satis_model

    # 将大的df插入拆分后的模型表
    all_unpack_model_spark_df.createOrReplaceTempView("unpack_model")
    insert_sql = """
        insert overwrite table edw.mod_sku_day_allot    -- # TODO: 表名
        select
            b.product_id
            , b.color_id
            , b.size_id
            , a.send_store_id
            , a.receive_store_id
            , a.date_send
            , a.date_rec_pred
            , a.send_qty
            , '{p_input_date_add_one_day}' as dec_day_date
            , current_timestamp as etl_time
        from unpack_model as a
        inner join edw.dim_product_sku as b
            on a.sku_id = b.sku_id
    """
    spark.execute_sql(insert_sql)

    # 写入标志位
    sql_model_flag = """
            select 
                'allot' as category
                , '{p_input_date_add_one_day}' as day_date
                , 1 as status
                , current_timestamp as etl_time
            union all
            select *
            from edw.model_flag
            where 
                day_date != '{p_input_date_add_one_day}'
                or category != 'allot'
        """
    model_flag_df = spark.execute_sql(sql_model_flag).toPandas()
    model_flag_spark_df = spark.spark.createDataFrame(model_flag_df)
    model_flag_spark_df.createOrReplaceTempView("tmp_model_flag")
    insert_sql = """
            insert overwrite table edw.model_flag
            select * from tmp_model_flag
        """
    spark.execute_sql(insert_sql)


def ai_check_weekday(weekday):
    """
    :param weekday: int, 1: Monday, ... 7: Sunday
    :return: bool
    """
    assert isinstance(weekday, int)
    param_list = sys.argv
    p_input_date = datetime.datetime.strptime(param_list[1], '%Y-%m-%d')
    return p_input_date.isocalendar()[2] == weekday


if __name__ == '__main__':
    if ai_check_weekday(1):
        # run at Monday end
        file_name = os.path.basename(__file__)
        spark = SparkInit(file_name)
        start_time = time.time()
        main()
        end_time = time.time()
        print(end_time - start_time)
