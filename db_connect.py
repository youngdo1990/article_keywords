# -*- coding: utf-8 -*-

import pymysql

# 데이터베이스 접속 정보
HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PSSWD = "root"
DB = "snuh_mart_db"
#데이터베이스 접속 정보

# MySQL 연결
def db_connection():
    return pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PSSWD, db=DB)
# MySQL 연결