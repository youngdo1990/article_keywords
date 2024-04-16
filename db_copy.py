# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-

from db_connect import *
from tools import *
from text_cleaner import * 
from frequency_counter import *
import pandas as pd


PROJECT_TABLE = "FCT_PROJECT_ABSTRACT"
DOMESTIC_TABLE = "FCT_PATENT_DOMESTIC"
OVERSEAS_TABLE = "FCT_PATENT_OVERSEAS"
PAPER_TABLE = "FCT_PAPER_ABSTRACT"
PAPER_KEYWORDS = "FCT_PAPER_KEYWORDS"
PROJECT_KEYWORDS = "FCT_PROJECT_KEYWORDS"
DOMESTIC_KEYWORDS = "FCT_PATENT_DOMESTIC_KEYWORDS"
OVERSEAS_KEYWORDS = "FCT_PATENT_OVERSEAS_KEYWORDS"
    

def investigation_task_db():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT RES_NO, TITLE_KR, TITLE_EN, ABSTRACT_TEASER, EFFECT_TEASER, KEYWORD_KR, KEYWORD_EN FROM " + PROJECT_TABLE
    cur.execute(query)
    result = cur.fetchall()

    res_no = []
    title_kr = []
    title_en = []
    abstract_teaser = []
    effect_teaser = []
    keyword_kr = []
    keyword_en = []

    for r in result:
        res_no.append(r[0])
        title_kr.append(r[1])
        title_en.append(r[2])
        abstract_teaser.append(r[3])
        effect_teaser.append(r[4])
        keyword_kr.append(r[5])
        keyword_en.append(r[6])

    df = pd.DataFrame(columns=["RES_NO", "TITLE_KR", "TITLE_EN", "ABSTRACT_TEASER", "EFFECT_TEASER", "KEYWORD_KR", "KEYWORD_EN"])

    df["RES_NO"] = res_no
    df["TITLE_KR"] = title_kr
    df["TITLE_EN"] = title_en
    df["ABSTRACT_TEASER"] = abstract_teaser
    df["EFFECT_TEASER"] = effect_teaser
    df["KEYWORD_KR"] = keyword_kr
    df["KEYWORD_EN"] = keyword_en
    df.to_excel(PROJECT_TABLE +  ".xlsx", index=False)
    conn.close()   
    return


def investigation_keywords():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT RES_NO, KEYWORD_EN, KEYWORD_KR, FREQ FROM " + PROJECT_KEYWORDS
    cur.execute(query)
    result = cur.fetchall()

    res_no = []
    keyword_en = []
    keyword_kr = []
    freq = []

    for r in result:
        res_no.append(r[0])
        keyword_en.append(r[1])
        keyword_kr.append(r[2])
        freq.append(r[3])

    df = pd.DataFrame(columns=["RES_NO", "KEYWORD_EN", "KEYWORD_KR", "FREQ"])

    df["RES_NO"] = res_no
    df["KEYWORD_EN"] = keyword_en
    df["KEYWORD_KR"] = keyword_kr
    df["FREQ"] = freq
    df.to_excel(PROJECT_KEYWORDS +  ".xlsx", index=False)
    conn.close()   
    return


def domestic_patent_db():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT MST_ID, INVENTION_TITLE_KO, ABSTRACT_CONTENTS FROM " + DOMESTIC_TABLE
    cur.execute(query)
    result = cur.fetchall()

    mst_id = []
    invention_title_ko = []
    abstract_content = []

    for r in result:
        mst_id.append(r[0])
        invention_title_ko.append(r[1])
        abstract_content.append(r[2])

    df = pd.DataFrame(columns=["MST_ID", "INVENTION_TITLE_KO", "INVENTION_TITLE_EN", "ABSTRACT_CONTENTS"])

    df["MST_ID"] = mst_id
    df["INVENTION_TITLE_KO"] = invention_title_ko
    df["ABSTRACT_CONTENTS"] = abstract_content
    df.to_excel(DOMESTIC_TABLE +  ".xlsx", index=False)
    conn.close()   
    return


def domestic_patent_keyword():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT MST_ID, KEYWORD, FREQ FROM " + DOMESTIC_KEYWORDS
    cur.execute(query)
    result = cur.fetchall()

    mst_id = []
    keyword = []
    freq = []

    for r in result:
        mst_id.append(r[0])
        keyword.append(r[1])
        freq.append(r[2])

    df = pd.DataFrame(columns=["MST_ID", "KEYWORD", "FREQ"])

    df["MST_ID"] = mst_id
    df["KEYWORD"] = keyword
    df["FREQ"] = freq
    df.to_excel(DOMESTIC_KEYWORDS +  ".xlsx", index=False)
    conn.close()   
    return


def overseas_patent_db():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT MST_ID, INVENTION_TITLE_KO, INVENTION_TITLE_EN, ABSTRACT_CONTENTS FROM " + OVERSEAS_TABLE
    cur.execute(query)
    result = cur.fetchall()

    mst_id = []
    invention_title_ko = []
    invention_title_en = []
    abstract_content = []

    for r in result:
        mst_id.append(r[0])
        invention_title_ko.append(r[1])
        invention_title_en.append(r[2])
        abstract_content.append(r[3])

    df = pd.DataFrame(columns=["MST_ID", "INVENTION_TITLE_KO", "ABSTRACT_CONTENTS"])

    df["MST_ID"] = mst_id
    df["INVENTION_TITLE_KO"] = invention_title_ko
    df["INVENTION_TITLE_EN"] = invention_title_en
    df["ABSTRACT_CONTENTS"] = abstract_content
    df.to_excel(OVERSEAS_TABLE +  ".xlsx", index=False)   
    conn.close()
    return


def overseas_patent_keyword():
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT MST_ID, KEYWORD_EN, KEYWORD_KR, FREQ FROM " + OVERSEAS_KEYWORDS
    cur.execute(query)
    result = cur.fetchall()

    mst_id = []
    keywords_en = []
    keywords_kr = []
    freq = []

    for r in result:
        mst_id.append(r[0])
        keywords_en.append(r[1])
        keywords_kr.append(r[2])
        freq.append(r[3])

    df = pd.DataFrame(columns=["MST_ID", "KEYWORD_EN", "KEYWORD_KR", "FREQ"])

    df["MST_ID"] = mst_id
    df["KEYWORD_EN"] = keywords_en
    df["KEYWORD_KR"] = keywords_kr
    df["FREQ"] = freq
    df.to_excel(OVERSEAS_KEYWORDS +  ".xlsx", index=False)   
    conn.close()
    return


def paper_db():
    conn = db_connection()
    cur = conn.cursor()
    query = query = "SELECT PMID, CN, TITLE, ABSTRACT FROM " + PAPER_TABLE
    cur.execute(query)
    result = cur.fetchall()

    pmid = []
    cn = []
    title = []
    abstract = []

    for r in result:
        pmid.append(r[0])
        cn.append(r[1])
        title.append(r[2])
        abstract.append(r[3])

    df = pd.DataFrame(columns=["PMID", "CN", "TITLE", "ABSTRACT"])

    df["PMID"] = pmid
    df["CN"] = cn
    df["TITLE"] = title
    df["ABSTRACT"] = abstract
    df.to_excel(PAPER_TABLE +  ".xlsx", index=False)   
    conn.close()
    return


def paper_keywords():
    DF_MAX_SIZE = 500000
    conn = db_connection()
    cur = conn.cursor()
    query = "SELECT PMID, CN, KEYWORD, FREQ FROM " + PAPER_KEYWORDS
    cur.execute(query)
    result = cur.fetchall()

    pmid = []
    cn = []
    keyword = []
    freq = []

    for r in result:
        pmid.append(r[0])
        cn.append(r[1])
        keyword.append(r[2])
        freq.append(r[3])

    df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])

    df["PMID"] = pmid
    df["CN"] = cn
    df["KEYWORD"] = keyword
    df["FREQ"] = freq
    
    count = 1
    
    while len(df) > DF_MAX_SIZE:
        print(len(df))
        temp_df = df.iloc[:DF_MAX_SIZE]
        df = df.iloc[DF_MAX_SIZE:]
        print(len(df))
        temp_df.to_excel(PAPER_KEYWORDS + str(count) + ".xlsx", index=False)
        count += 1
    if not df.empty:
        df.to_excel(PAPER_KEYWORDS + str(count) + ".xlsx", index=False)

    

    conn.close()
    return


if __name__ == "__main__":
    # investigation_task_db()
    domestic_patent_db()
    # overseas_patent_db()
    # paper_db()
    # investigation_keywords()
    # domestic_patent_keyword()
    # overseas_patent_keyword()
    # paper_keywords()