import datetime
from datetime import date
import requests
import time
import json
from requests.auth import HTTPBasicAuth
import psycopg2
from pyhive import presto
import configparser

config = configparser.ConfigParser()
# config['DEFAULT'] = {'ServerAliveInterval': '45',
# 'Compression': 'yes',
# 'CompressionLevel': '9'}
#
# with open('app_conf.ini', 'r') as configfile:
config.read('app_conf.ini')



x = config["gpdb"]["gpdb_port"]
print(x)
print(type(x))




#
# gpdb_user_name=inj_cfg["gpdb_user_name"]
# gpdb_pass=inj_cfg["gpdb_pass"]
# gpdb_host=inj_cfg["gpdb_host"]
# gpdb_port=inj_cfg["gpdb_port"]
# gpdb_database=inj_cfg["gpdb_database"]
#
#
# conn = presto.connect(username="hive", host="10.164.7.201", port="8888", catalog="hive", schema="default")
# # conn= psycopg2.connect(user=gpdb_user_name, password=gpdb_pass, host=gpdb_host, port=gpdb_port, database=gpdb_database)
# # start_ip = inj_cfg["ip_first_segment_start_point"]+inj_cfg["ip_second_segment_start_point"]+inj_cfg["ip_third_segment_start_point"]
#
# # enums=[5,25,6,10,2]
#
#
# today = date.today()
# yesterday = today - datetime.timedelta(days=1)
# current_date = today.strftime("%Y_%m_%d")
# yesterday_date= yesterday.strftime("%Y_%m_%d")
# day = today.strftime("%d")
# print(day)
#
# #### find relevant tables
#
# query = "select table_name from information_schema.tables where table_name like '%xdr%' and table_name not like '%index%' and (table_name  like '%" + str(current_date) + "%' or table_name  like '%" + str(yesterday_date) + "%')"
#
#
# query = "select table_name from information_schema.tables where table_name like '%xdr%' and table_name not like '%index%' and (table_name  like '%2021_11_04%' or table_name  like '%2021_11_06%')"
#
# product_types_to_verify = inj_cfg["hive_enums"]
#
#
# print(query)
# cursor = conn.cursor()
# cursor.execute(query)
# tables = cursor.fetchall()
# for i in range(len(product_types_to_verify)):
#     amount = 0
#     for j in range(len(tables)):
#         query = "select count (distinct product_id) as amount from " + str((tables[j])[0]) + " where product_type= " + str(product_types_to_verify[i]) + " and site_id=1 and identifiers like '%123.122.123.%'"
#         print(query)
#         cursor = conn.cursor()
#         cursor.execute(query)
#         records = cursor.fetchone()
#         amount = amount + records[0]
#     print(amount)
#
#
# print(tables)
# print(type(tables))
# print(len(tables))
# print((tables[0])[0])
#
#
#
# # def url_checker():
# #   url_to_test = input("url to test: ")
# #   for i in range(5):
# #         response = requests.get(str(url_to_test))
# #         if response.status_code != 200:
# #             print("somthing is not ok")
# #         else:
# #             print('evrything is ok"')
# #   time.sleep(5)
# #
# #
# # url_checker()
# # http://10.164.139.187:9999/interfaces
#
#
# #
#




