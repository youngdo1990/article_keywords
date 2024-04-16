# -*- coding: utf-8 -*-

from db_connect import *
import os
import pandas as pd
from langdetect import detect
from tqdm import *


PROJECT_TABLE = "FCT_PROJECT_KEYWORDS"
DOMESTIC_PATENT_TABLE = "FCT_PATENT_DOMESTIC_KEYWORDS"
OVERSEAS_PATENT_TABLE = "FCT_PATENT_OVERSEAS_KEYWORDS"
PAPER_TABLE = "FCT_PAPER_KEYWORDS"


def create_project_table():
    conn = db_connection()
    cur = conn.cursor()
    # 중복된 정보 예방
    query = "DROP TABLE IF EXISTS " + PROJECT_TABLE
    cur.execute(query)
    # 과제 키워드 테이블
    query = "CREATE TABLE IF NOT EXISTS " + PROJECT_TABLE + " ("
    query += "RES_NO VARCHAR(10) NOT NULL,"
    query += "KEYWORD_EN VARCHAR(50) DEFAULT NULL,"
    query += "KEYWORD_KR VARCHAR(30) DEFAULT NULL,"
    query += "FREQ INT NOT NULL"
    query += ")"
    cur.execute(query)
    conn.close()
    return


def create_patent_table():
    conn = db_connection()
    cur = conn.cursor()
    # 중복된 정보 예방
    query = "DROP TABLE IF EXISTS " + DOMESTIC_PATENT_TABLE
    cur.execute(query)
    # 국내 특허 키워드 테이블
    query = "CREATE TABLE IF NOT EXISTS " + DOMESTIC_PATENT_TABLE + " ("
    query += "MST_ID VARCHAR(18) NOT NULL,"
    query += "KEYWORD VARCHAR(30) DEFAULT NULL,"
    query += "FREQ INT NOT NULL"
    query += ")"
    cur.execute(query)
    # 중복된 정보 예방
    query = "DROP TABLE IF EXISTS " + DOMESTIC_PATENT_TABLE
    cur.execute(query)
    # 외국 특허 키워드 테이블
    query = "CREATE TABLE IF NOT EXISTS " + DOMESTIC_PATENT_TABLE + " ("
    query += "MST_ID VARCHAR(18) NOT NULL,"
    query += "KEYWORD_EN VARCHAR(50) DEFAULT NULL,"
    query += "KEYWORD_KR VARCHAR(30) DEFAULT NULL,"
    query += "FREQ INT NOT NULL"
    query += ")"
    cur.execute(query)
    conn.close()
    return


def create_paper_table():
    conn = db_connection()
    cur = conn.cursor()
    # 중복된 정보 예방
    query = "DROP TABLE IF EXISTS " + PAPER_TABLE
    cur.execute(query)
    # 논문 키워드 테이브
    query = "CREATE TABLE IF NOT EXISTS " + PAPER_TABLE + " ("
    query += "PMID VARCHAR(10) NOT NULL,"
    query += "CN VARCHAR(30) NOT NULL,"
    query += "KEYWORD VARCHAR(50) DEFAULT NULL,"
    query += "FREQ INT NOT NULL"
    query += ")"
    cur.execute(query)
    conn.close()
    return


def load_patent_info():
    create_patent_table()
    source = os.path.join("특허", "domestic", "patent_keywords_kor.xlsx")
    print(source)
    df = pd.read_excel(source)
    print(df.head())
    conn = db_connection()
    cur = conn.cursor()

    print(df.columns)
    for i in tqdm(df.index):
        query = "INSERT INTO " + DOMESTIC_PATENT_TABLE
        query += " (MST_ID, KEYWORD, FREQ) VALUES "
        query += "('" + str(df["MST_ID"][i]) + "', '" + str(df["KEYWORD"][i]) + "', '" + str(df["FREQ"][i]) + "')"
        cur.execute(query)
        conn.commit()

    source = os.path.join("특허", "overseas", "patent_keywords_total.xlsx")
    df = pd.read_excel(source)
    for i in df.index:
        if detect(df["KEYWORD"][i]) == "ko":
            ko_keyword = df["KEYWORD"][i]
            en_keyword = ""
        else:
            en_keyword = df["KEYWORD"][i]
            ko_keyword = ""
        
        query = "INSERT INTO " + OVERSEAS_PATENT_TABLE
        query += " (MST_ID, KEYWORD_EN, KEYWORD_KR, FREQ) VALUES "
        query += "('" + str(df["MST_ID"][i]) 
        query += "', '" + en_keyword + "', '" + ko_keyword + "', '" + str(df["FREQ"][i]) + "')"
        cur.execute(query)
        conn.commit()

    conn.close()
    return


def load_project_info():
    create_project_table()
    source = os.path.join("연구과제", "TOTAL", "project_keywords_total.xlsx")
    print(source)
    df = pd.read_excel(source)
    
    # print(df.loc[df["RES_NO"] == "2016-2930"])
    
    print(df.head())
    conn = db_connection()
    cur = conn.cursor()

    for i in tqdm(df.index):
        try:
            if detect(df["KEYWORD"][i]) == "ko":
                ko_keyword = df["KEYWORD"][i]
                en_keyword = ""
            else:
                en_keyword = df["KEYWORD"][i]
                ko_keyword = ""
            
            query = "INSERT INTO " + PROJECT_TABLE
            query += " (RES_NO, KEYWORD_EN, KEYWORD_KR, FREQ) VALUES "
            query += "('" + str(df["RES_NO"][i]) + "', '" + en_keyword + "', '" + ko_keyword + "', '" + str(df["FREQ"][i]) + "')"
            cur.execute(query)
            conn.commit()
        except:
            pass

    conn.close()
    return


def load_paper_info():
    create_paper_table()
    source = os.path.join("논문", "paper_keywords.xlsx")
    print(source)
    df = pd.read_excel(source)
    print(df.head())
    conn = db_connection()
    cur = conn.cursor()
    df = df.fillna("")
    for i in tqdm(df.index):
        if df["PMID"][i] == None:
            pmid = ""
        else:
            pmid = str(df["PMID"][i])
            pmid = pmid.replace(".0", "")
            
        if df["CN"][i] == None:
            cn = ""
        else:
            cn = df["CN"][i]
        query = "INSERT INTO " + PAPER_TABLE
        query += " (PMID, CN, KEYWORD, FREQ) VALUES "
        query += "('" + str(pmid) + "', '" + str(cn) + "', '" + str(df["KEYWORD"][i]) + "', '" + str(df["FREQ"][i]) + "')"
        cur.execute(query)
        conn.commit()

    conn.close()
    return


if __name__ == "__main__":
    # create_tables()
    # load_patent_info()
    # load_project_info()
    load_paper_info()