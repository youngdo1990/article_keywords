# -*- coding: utf-8 -*-

from tools import *
from text_cleaner import * 
from frequency_counter import *
import os

OUTPUT_FOLDER = "특허2"
DOMESTIC_TABLE = "fct_patent_domestic"
OVERSEAS_TABLE = "fct_patent_overseas"


def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


def domestic_patent_keywords(result=None):
    # source_file = "FCT_PATENT_DOMESTIC.xlsx"
    # df = pd.read_excel(source_file)
    # df.fillna("", inplace=True)
    df = get_dataframes_tuple(result)
    if result.empty:
        result = get_dataframes_tuple(df)
    else:
        result = get_dataframes_tuple(result)

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    mst_id = []
    keyword = []
    freq = []

    for r in tqdm(result):
        invention_title = r[1]
        abstract_contents = r[3]
        invention_title_en = r[2]
        
        title_keywords = kr_model.extract_keywords(invention_title)
        abstract_contents_keywords = []
        keywords = [w[0] for w in title_keywords]
        if abstract_contents != None:
            abstract_contents_keywords = kr_model.extract_keywords(abstract_contents)
            for w in abstract_contents_keywords:
                if w[0] not in keywords:
                    keywords.append(w[0])
        keywords = clean_ko_keywords(keywords)
        eng_keywords = eng_model.extract_keywords(invention_title_en)
        eng_keywords = [word[0] for word in eng_keywords if word[1] > 0.6]
        eng_keywords = clean_eng_keywords(eng_keywords)
        eng_keywords_freq = [eng_freq_counter(f, invention_title_en) for f in eng_keywords]
        eng_keywords_dict = {i:j for i, j in zip(eng_keywords, eng_keywords_freq)}
        eng_keywords_dict = {key:eng_keywords_dict[key] for key in eng_keywords_dict if eng_keywords_dict[key] > 0}
        keywords_freq = [kor_freq_counter(f, invention_title) for f in keywords]
        keywords_dict = {i:j for i, j in zip(keywords, keywords_freq)}
        for word in eng_keywords_dict:
            if word not in keywords_dict:
                keywords_dict[word] = eng_keywords_dict[word]

        keywords_dict = {key:keywords_dict[key] for key in keywords_dict if keywords_dict[key] > 0}
        keywords_dict = dict(sorted(keywords_dict.items(), key=lambda item:item[1], reverse=True))
        
        for k in keywords_dict:
            mst_id.append(r[0])
            keyword.append(k)
            freq.append(keywords_dict[k])
    # 사전별 데이터 프레임 만들기
    keywords_df = pd.DataFrame(columns=["MST_ID", "KEYWORD", "FREQ"])
    keywords_df["MST_ID"] = mst_id
    keywords_df["KEYWORD"] = keyword
    keywords_df["FREQ"] = freq

    if not os.path.exists(os.path.join(OUTPUT_FOLDER, "domestic")):
        os.makedirs(os.path.join(OUTPUT_FOLDER, "domestic"))

    keywords_df.to_excel(os.path.join(OUTPUT_FOLDER, "domestic", "patent_keywords_kor.xlsx"), index=False)
    
    return keywords_df


def overseas_patent_keywords(result=None):
    # source_file = "FCT_PATENT_OVERSEAS.xlsx"
    df = get_dataframes_tuple(result)
    if result.empty:
        result = get_dataframes_tuple(df)
    else:
        result = get_dataframes_tuple(result)

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    
    ko_mst_id = []
    ko_keyword = []
    ko_freq = []
    en_mst_id = []
    en_keyword = []
    en_freq = []
    total_mst_id = []
    total_keyword = []
    total_freq = []

    for r in tqdm(result):
        invention_title_ko = r[1]
        invention_title_en = r[2]
        abstract_contents = r[3]
        title_ko_keywords = kr_model.extract_keywords(invention_title_ko)
        title_en_keywords = eng_model.extract_keywords(invention_title_en)
        abstract_contents_keywords = []
        ko_keywords = [w[0] for w in title_ko_keywords]
        eng_keywords = [w[0] for w in title_en_keywords]

        if abstract_contents != None:
            abstract_contents_keywords = eng_model.extract_keywords(abstract_contents)
            for w in abstract_contents_keywords:
                if w[0] not in eng_keywords:
                    eng_keywords.append(w[0])
        else:
            abstract_contents = ""
        # 언어마다 키워드 깨끗하게 하다
        ko_keywords = clean_ko_keywords(ko_keywords)
        eng_keywords = clean_eng_keywords(eng_keywords)
        ko_keywords_freq = [kor_freq_counter(f, invention_title_ko) for f in ko_keywords]
        eng_keywords_freq = [eng_freq_counter(f, invention_title_en + "\n" + abstract_contents) for f in eng_keywords]
        # 키워드 빈도 사전
        ko_keywords_dict = {i:j for i, j in zip(ko_keywords, ko_keywords_freq)}
        ko_keywords_dict = {key:ko_keywords_dict[key] for key in ko_keywords_dict if ko_keywords_dict[key] > 0}
        eng_keywords_dict = {i:j for i, j in zip(eng_keywords, eng_keywords_freq)}
        eng_keywords_dict = {key:eng_keywords_dict[key] for key in eng_keywords_dict if eng_keywords_dict[key] > 0}
        # 사전 합치기
        total_keywords_dict = eng_keywords_dict
        for k in ko_keywords_dict:
            if k not in total_keywords_dict:
                total_keywords_dict[k] = ko_keywords_dict[k]
            else:
                total_keywords_dict[k] += ko_keywords_dict[k]
        # 빈도로 배열
        ko_keywords_dict = dict(sorted(ko_keywords_dict.items(), key=lambda item:item[1], reverse=True))
        eng_keywords_dict = dict(sorted(eng_keywords_dict.items(), key=lambda item:item[1], reverse=True))
        total_keywords_dict = dict(sorted(total_keywords_dict.items(), key=lambda item:item[1], reverse=True))
        
        for k in ko_keywords_dict:
            ko_mst_id.append(r[0])
            ko_keyword.append(k)
            ko_freq.append(ko_keywords_dict[k])

        for k in eng_keywords_dict:
            en_mst_id.append(r[0])
            en_keyword.append(k)
            en_freq.append(eng_keywords_dict[k])

        for k in total_keywords_dict:
            total_mst_id.append(r[0])
            total_keyword.append(k)
            total_freq.append(total_keywords_dict[k])
    
    ko_df = pd.DataFrame(columns=["MST_ID", "KEYWORD", "FREQ"])
    ko_df["MST_ID"] = ko_mst_id
    ko_df["KEYWORD"] = ko_keyword
    ko_df["FREQ"] = ko_freq
    en_df = pd.DataFrame(columns=["MST_ID", "KEYWORD", "FREQ"])
    en_df["MST_ID"] = en_mst_id
    en_df["KEYWORD"] = en_keyword
    en_df["FREQ"] = en_freq
    total_df = pd.DataFrame(columns=["MST_ID", "KEYWORD", "FREQ"])
    total_df["MST_ID"] = en_mst_id
    total_df["KEYWORD"] = total_keyword
    total_df["FREQ"] = total_freq

    if not os.path.exists(os.path.join(OUTPUT_FOLDER, "overseas")):
        os.makedirs(os.path.join(OUTPUT_FOLDER, "overseas"))

    ko_df.to_excel(os.path.join(OUTPUT_FOLDER, "overseas", "patent_keywords_kor.xlsx"), index=False)
    en_df.to_excel(os.path.join(OUTPUT_FOLDER, "overseas", "patent_keywords_en.xlsx"), index=False)
    total_df.to_excel(os.path.join(OUTPUT_FOLDER, "overseas", "patent_keywords_total.xlsx"), index=False)
    
    return total_df


if __name__ == "__main__":
    domestic_patent_keywords()
    overseas_patent_keywords()