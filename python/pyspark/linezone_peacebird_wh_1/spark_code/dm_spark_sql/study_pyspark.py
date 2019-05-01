# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     study_pyspark
   Description :
   Author :       yangming
   date：          2018/9/30p-
-------------------------------------------------
   Change Activity:
                   2018/9/3:
-------------------------------------------------
"""

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from os.path import abspath
from pyspark import SparkConf
from pyspark.sql.functions import udf
import datetime
from datetime import timedelta


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
        # 如p_rst_skc_overall插入步骤
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')

        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()

        # self.register_udf()

        # 获取脚本执行需要的参数
        self.params_dict = {}

    def register_udf(self):
        """
        注册udf使用
        :return: 
        """
        self.spark.udf.register('date_trunc', date_trunc)

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
        self.spark.sql(sql_to_execute)

    def return_df(self, sql):
        """
        spark引擎执行sql语句并返回dataframe
        :param
        sql:
        :return:
        df
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
    # TODO 以后兼容各种日期格式，现在只支持 2018-09-12 标准日期字符串
    # date_obj = parse(date_str)
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == 'week':
        res = date_obj - timedelta(days=(date_obj.isocalendar()[2] - 1))
    elif interval == 'month':
        res = datetime.date(date_obj.year, date_obj.month, 1)
    elif interval == 'year':
        res = datetime.date(date_obj.year, 1, 1)
    else:
        raise Exception("interval must be ['week', 'month', 'year']")
    return res.strftime('%Y-%m-%d')

if __name__ == '__main__':
    foo = SparkInit('ym')
    foo.spark.sql("select date_trunc('week', '2018-09-13')")
