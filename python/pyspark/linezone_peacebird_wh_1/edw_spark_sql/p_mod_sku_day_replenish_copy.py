# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird
# Filename: p_mod_sku_day_replenish
# Author: zsm
# Date: 2018/11/24 10:27
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
from pyspark.sql.functions import udf, concat
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
    # 取总仓CB37的库存
    sql_tmp_warehouse_stock = '''
        select 
            org_id
            , product_id
            , color_id
            , size_id
            , stock_qty
        from edw.mid_day_end_stock
        where stock_date = '{p_input_date}' 
            and org_id = '420867'
    '''
    spark.create_temp_table(sql_tmp_warehouse_stock, "tmp_warehouse_stock")

    # 生成主款/替代款组合表
    '''
    主款  替代款  优先级
    sku1    sku1    0
    sku1    a1      1
    sku1    a2      2
    ---------------------------
    '''
    sql_tmp_main_alter_sku = '''
        select 
            m_productalias_id as main_skuid 
            , m_productalias_id as main_alter_skuid 
            , '0' as priority
        from peacebird.st_sku_relate_view
        group by m_productalias_id
        union all 
        select 
            m_productalias_id as main_skuid 
            , m_productalias2_id as main_alter_skuid
            ,priority
        from peacebird.st_sku_relate_view
    '''
    spark.create_temp_table(sql_tmp_main_alter_sku, "main_alter_sku")

    # 取主款及替代款的日末库存
    sql_main_alter_sku_stock = '''
        select 
            coalesce(main_alter_sku.main_skuid, dim_sku.sku_id) as main_skuid
            , coalesce(main_alter_sku.main_alter_skuid, dim_sku.sku_id) as main_alter_skuid
            , coalesce(main_alter_sku.priority, '0') as priority
            , stock.stock_qty 
        from tmp_warehouse_stock as stock
        inner join edw.dim_product_sku as dim_sku
            on dim_sku.product_id = stock.product_id
            and dim_sku.color_id = stock.color_id 
            and dim_sku.size_id = stock.size_id
        left join main_alter_sku 
            on main_alter_sku.main_alter_skuid = dim_sku.sku_id
    '''
    main_alter_sku_stock = spark.execute_sql(sql_main_alter_sku_stock)

    # 原始模型表
    sql_origin_model = """
        select
            sku.sku_id
            , a.send_org_id
            , a.receive_store_id
            , a.date_send
            , a.date_rec_pred
            , a.send_qty
            , a.dec_day_date
        from (
            select * 
            from edw.mod_sku_day_replenish_model
            where dec_day_date = '{p_input_date_add_one_day}'
        ) as a
        inner join edw.dim_product_sku as sku         
            on a.product_id = sku.product_id 
            and a.color_id = sku.color_id 
            and a.size_id = sku.size_id
    """
    origin_model = spark.execute_sql(sql_origin_model)

    unpack_model(main_alter_sku_stock, origin_model)


def unpack_model(main_alter_sku_stock, origin_model):
    main_alter_sku_stock = main_alter_sku_stock.toPandas()
    origin_model = origin_model.toPandas()
    unpack_list = []
    # 每一条模型建议
    for i in range(origin_model.shape[0]):
        suggest_df = origin_model[i:i + 1]
        sku_id = suggest_df['sku_id'].values[0]
        send_qty_init = suggest_df['send_qty'].values[0]
        send_qty = send_qty_init
        stock_order_df = main_alter_sku_stock[main_alter_sku_stock['main_skuid'] == sku_id] \
            .sort_values(by='priority')
        for j in range(stock_order_df.shape[0]):
            alter_stock_df = stock_order_df[j:j + 1]
            alter_stock_qty = alter_stock_df['stock_qty'].values[0]
            alter_sku_id = alter_stock_df['main_alter_skuid'].values[0]
            index = main_alter_sku_stock[main_alter_sku_stock['main_alter_skuid'] == alter_sku_id]. \
                index.tolist()[0]
            if alter_stock_qty >= send_qty:
                # if j == 0:
                #     unpack_list.append(suggest_df)
                # else:
                # 把新生成的一条放入原始模型df
                extend_suggest_df = suggest_df.replace(sku_id, alter_sku_id, inplace=False) \
                    .replace(send_qty_init, send_qty)
                unpack_list.append(extend_suggest_df)
                # 相应的库存赋值为alter_stock_qty-send_qty
                main_alter_sku_stock[index: index+1]['stock_qty'] = alter_stock_qty - send_qty
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
                    # 相应替代款的库存赋值为0
                    main_alter_sku_stock[index: index+1]['stock_qty'] = 0
                    send_qty = send_qty - alter_stock_qty
    # 把所有unpack_list中的df合并为大的df
    unpack_model_df = pd.concat(unpack_list, axis=0)
    unpack_model_spark_df = spark.spark.createDataFrame(unpack_model_df)

    # 将大的df插入拆分后的模型表
    unpack_model_spark_df.createOrReplaceTempView("unpack_model")
    insert_sql = """
        insert overwrite table edw.mod_sku_day_replenish 
        select
            b.product_id
            , b.color_id
            , b.size_id
            , a.send_org_id
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
            'replenish' as category
            , '{p_input_date_add_one_day}' as day_date
            , 1 as status
            , current_timestamp as etl_time
        union all
        select *
        from edw.model_flag
        where 
            day_date != '{p_input_date_add_one_day}'
            or category != 'replenish'
    """
    model_flag_df = spark.execute_sql(sql_model_flag).toPandas()
    model_flag_spark_df = spark.spark.createDataFrame(model_flag_df)
    model_flag_spark_df.createOrReplaceTempView("tmp_model_flag")
    insert_sql = """
        insert overwrite table edw.model_flag
        select * from tmp_model_flag
    """
    spark.execute_sql(insert_sql)


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    start_time = time.time()
    main()
    end_time = time.time()
    print(end_time - start_time)
