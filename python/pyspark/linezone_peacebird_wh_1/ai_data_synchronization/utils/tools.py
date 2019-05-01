#!/usr/bin/env python
# encoding: utf-8
import calendar
import os
import datetime
import time
import numpy
import hashlib
import math
from random import Random
from decimal import Decimal, ROUND_HALF_UP
from datetime import timedelta
from sqlalchemy import text as _
import pandas as pd
from ai_data_synchronization.utils.publictool import open_session, engine
from ai_data_synchronization.constants.ResponseCodeMsgConstants import RESPONSE_CODE_MSG_CONSTANTS
from ai_data_synchronization.exceptions.AiDataSynException import QueryNothingException, DatabaseException
from ai_data_synchronization.config import Log


# 获取文件上传路径
def get_upload_file_path(file_name):
    """
    :param file_name: 文件名
    :return:
    """
    today = datetime.date.today().day
    path = os.path.join(os.path.join("media", 'upload'), str(today))
    if not os.path.exists(path):
        os.makedirs(path)
    path_name = os.path.join(path, file_name)
    return path_name


def jm_md5(string):
    """
    md5加密
    return:加密结果转成32位字符串并小写
    """
    md5 = hashlib.md5()
    md5.update(string.encode('utf-8'))
    md5_str = md5.hexdigest()
    return md5_str.lower()


def get_idcode():
    """
    生成随机的表id（数据库表的id）
    :return:
    """
    date = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    n = 4
    random = Random()
    return date + ''.join(random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') for x in range(n))


# /////////////////////////////////////////////////////日期函数/////////////////////////////////////////////////////////
def query_day_date(p_input_date, n, strf):
    """
    :param n: 今天加几天
    返回今天加n天后的日期
    :return:
    """
    p_input_date = datetime.datetime.strptime(p_input_date, '%Y-%m-%d')
    day_date = p_input_date + datetime.timedelta(n)
    day_date = day_date.strftime(strf)
    return day_date


def get_week_period(week, flag="str"):
    year = int(time.strftime("%Y"))
    date_to = datetime.datetime(year=year, month=1, day=1) + datetime.timedelta(days=int(week) * 7)
    date_from = datetime.datetime(year=year, month=1, day=1) + datetime.timedelta(days=int(week) * 7 - 6)
    if flag == "str":
        return date_from.strftime("%Y%m%d"), date_to.strftime("%Y%m%d")
    else:
        return date_from, date_to


# 获取下周周数
def get_next_week():
    return (datetime.datetime.now() + datetime.timedelta(days=7)).strftime("%W")


def query_some_monday(p_input_date):
    """
    返回上周一的日期
    :return:
    """
    p_input_date = datetime.datetime.strptime(p_input_date, '%Y%m%d')
    this_monday = (p_input_date - timedelta(days=(p_input_date.isocalendar()[2] - 1))).strftime('%Y%m%d')
    return this_monday


# ///////////////////////////////////////////////////////异常/数据库操作///////////////////////////////////////////////////////////
def if_query_nothing(info):
    """
    判断输入信息长度
    若为 0 则表示未查到数据
    抛出异常
    :param info:
    :return:
    """
    try:
        if info is None or len(info) == 0:
            raise QueryNothingException(
                RESPONSE_CODE_MSG_CONSTANTS.CODE_NO_INFO,
                RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_NO_INFO,
            )
    except QueryNothingException as e:
        Log.server_log.error(e)
        raise e


def get_db_data(sql, kwargs):
    try:
        with open_session() as s:
            try:
                db_data = s.execute(_(sql), kwargs).fetchall()
            except Exception as e:
                s.rollback()
                msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_SELECT_ERROR + '<' + str(e) + '>'
                raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_SELECT_ERROR, msg)
            else:
                return db_data
    except DatabaseException as e:
        Log.server_log.error(str(e))
        raise e


def update_db_data(sql, kwargs):
    try:
        with open_session() as s:
            try:
                db_data = s.execute(_(sql), kwargs)
            except Exception as e:
                s.rollback()
                msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_UPDATE_ERROR + '<' + str(e) + '>'
                raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_UPDATE_ERROR, msg)
    except DatabaseException as e:
        Log.server_log.error(str(e))
        raise e


def delete_db_data(sql, kwargs):
    try:
        with open_session() as s:
            try:
                db_data = s.execute(_(sql), kwargs)
            except Exception as e:
                s.rollback()
                msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_DELETE_ERROR + '<' + str(e) + '>'
                raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_DELETE_ERROR, msg)
    except DatabaseException as e:
        Log.server_log.error(str(e))
        raise e


def insert_db_data(sql, kwargs):
    try:
        with open_session() as s:
            try:
                db_data = s.execute(_(sql), kwargs)
            except Exception as e:
                s.rollback()
                msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_UPDATE_ERROR + '<' + str(e) + '>'
                raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_UPDATE_ERROR, msg)
    except DatabaseException as e:
        Log.server_log.error(str(e))
        raise e


def insert_db_df(target_table_name, data):
    try:
        data.to_sql(target_table_name, engine, index=False, if_exists='append')
    except Exception as e:
        msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_INSERT_ERROR + '<' + str(e) + '>'
        raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_INSERT_ERROR, msg)


# def get_oracle_data(sql, kwargs):
#     try:
#         with open_oracle() as s:
#             try:
#                 db_data = s.execute(_(sql), kwargs).fetchall()
#             except Exception as e:
#                 s.rollback()
#                 msg = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_DATABASE_SELECT_ERROR + '<' + str(e) + '>'
#                 raise DatabaseException(RESPONSE_CODE_MSG_CONSTANTS.CODE_DATABASE_SELECT_ERROR, msg)
#             else:
#                 return db_data
#     except DatabaseException as e:
#         Log.server_log.error(str(e))
#         raise e


# ////////////////////////////////////////////////////dataframe处理////////////////////////////////////////////////////
def float_to_decimal(n):
    if n:
        return Decimal(n)
    else:
        return n


def int64_to_float(n):
    return float(n)


def percent_div(up, down, ndigits=2, percent=True):
    if up == 0 or up is None or down is None:
        return 0
    try:
        if percent:
            return round((up * 1.0 / down) * 100, ndigits=ndigits)
        else:
            return round((up * 1.0 / down), ndigits=ndigits)
    except ZeroDivisionError:
        return 0


def df_columns_round(sr, rd):
    """
    数据库中可能会出现拿到的数字不是数字应有类型的情况，需要进行转换

    round 之后再 fillna 保证 round 不出错

    astype 之前 fillna 保证空值能正确被替换，否则 astype 之后空值变成 'None'
    等其他不容易确定的值

    某些需要保存小数点精度的 series 是经过相除的，可能得到 inf、nan 等不容易
    在 astype 之前替换的类型。在 astype 之后变成 'inf'、'nan' 容易替换
    :param sr:
    :param rd:
    :return:
    """
    if sr.dtype != numpy.float64:
        sr = sr.astype(float)
    sr = sr.round(rd)
    sr.fillna('-', inplace=True)
    sr = sr.astype(str)
    sr.replace('inf', '-', inplace=True)
    sr.replace('nan', '-', inplace=True)
    return sr


def divide_df_columns(sr1, sr2, rd):
    if sr1.dtype != numpy.float64:
        sr1 = sr1.astype(float)
    if sr2.dtype != numpy.float64:
        sr2 = sr2.astype(float)
    sr3 = sr1 / sr2
    return df_columns_round(sr3, rd)


def divide_df_rows(sr1, sr2, rd):
    sum1 = sr1.sum()
    sum2 = sr2.sum()
    avg = sum1/sum2
    avg = round_tool(avg, rd)
    if not math.isnan(avg):
        return float(avg)
    else:
        return '-'


def round_tool(num, n):
    zero = (n - 1) * '0'
    num = Decimal(num)
    num = Decimal(num.quantize(Decimal('.{}1'.format(zero)), rounding=ROUND_HALF_UP))
    return str(num)


if __name__ == '__main__':
    query_some_monday('2018-09-01')

