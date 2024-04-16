# -*- coding: utf-8 -*-

from db_connect import *
from tools import *
from text_cleaner import * 
from frequency_counter import *
import os
## RAKE
from rake_nltk import Rake


TABLE = "FCT_PROJECT_ABSTRACT"
OUTPUT_FOLDER = "연구과제2"
EXPECTED_KEYWORDS_NUMBER = 20
MIN_FREQ = 2


def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


def load_checkpoint():
    file = os.path.join(OUTPUT_FOLDER, "TOTAL", "project_keywords_total.xlsx")
    print(file)
    print(os.listdir(os.path.join(OUTPUT_FOLDER, "TOTAL")))
    if os.path.exists(file):
        df = pd.read_excel(file)
        df = df.fillna("")
        print(df.head())
        ids_list = df["RES_NO"].unique().tolist()
        return ids_list
    else:
        return []


def complement_with_rake(title, abstract, keywords, rake):
    text = title + "\n" + abstract
    rake.extract_keywords_from_text(text)
    rake_keywords = dict(sorted(rake.frequency_dist.items(), key=lambda item:item[1], reverse=True))
    # rake_keywords = {key for key in rake_keywords if key not in ENGLISH_STOPWORDS}
    rake_keywords = {key:rake_keywords[key] for key in rake_keywords if rake_keywords[key] > MIN_FREQ and key.isalpha() and key!="et" and key!="al" and key!="ad" and len(key)>1}
    for key in rake_keywords:
        if key not in keywords:
            keywords[key] = rake_keywords[key]
    keywords = dict(sorted(keywords.items(), key=lambda item:item[1], reverse=True))
    return keywords


def investigation_task_keywords(checkpoint=True, result=None):
    
    if result == None:
        source_file = "FCT_PROJECT_ABSTRACT"
        df = pd.read_excel(source_file)
        result = get_dataframes_tuple(df)
        if checkpoint:
            evaluated_ids = load_checkpoint()
            result = [r for r in result if r[0] not in evaluated_ids]
    else:
        result = get_dataframes_tuple(result)

    count = 0

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # result = result[:100]

    # 테이블 필드
    ko_res_no = []
    ko_keyword = []
    ko_freq = []
    en_res_no = []
    en_keyword = []
    en_freq = []
    total_res_no = []
    total_keyword = []
    total_freq = []
    # 테이블 필드

    rake = Rake()
    
    # result = result[:100]

    for r in tqdm(result):
        title_kr = r[1]
        title_en = r[2]
        abstract_teaser = r[3]
        efect_teaser = r[4]
        keyword_kr = r[5]
        keywords_en = r[6]

        title_kr_keywords = kr_model.extract_keywords(title_kr, top_n=40,  nr_candidates=80)
        title_en_keywords = []
        abstract_teaser_keywords = []
        if title_en:
            title_en_keywords = eng_model.extract_keywords(title_en, top_n=40,  nr_candidates=80)
        

        ko_keywords = [w[0] for w in title_kr_keywords]
        if abstract_teaser:
            abstract_teaser_keywords = kr_model.extract_keywords(abstract_teaser, top_n=40,  nr_candidates=80)
            for w in abstract_teaser_keywords:
                if w not in ko_keywords:
                    ko_keywords.append(w[0])
        
        if keyword_kr:
            keywords_key = kr_model.extract_keywords(keyword_kr, top_n=40,  nr_candidates=80)
            for w in keywords_key:
                if w not in ko_keywords:
                    ko_keywords.append(w[0])

        if efect_teaser:
            abstract_teaser_keywords = kr_model.extract_keywords(abstract_teaser, top_n=40,  nr_candidates=80)
            for w in abstract_teaser_keywords:
                if w not in ko_keywords:
                    ko_keywords.append(w[0])

        eng_keywords = [w[0] for w in title_en_keywords]
        if keywords_en != None:
                temp_text = keywords_en
                content_en_keyboards = eng_model.extract_keywords(keywords_en, top_n=40,  nr_candidates=80)
                for w in content_en_keyboards:
                    if w not in eng_keywords:
                        eng_keywords.append(w[0])
        ko_keywords = clean_ko_keywords(ko_keywords)
        eng_keywords = clean_eng_keywords(eng_keywords)
        if abstract_teaser == None:
            ko_keywords_freq = [kor_freq_counter(f, title_kr) for f in ko_keywords]
        else:
            temp_ko_text = title_kr + "\n" + abstract_teaser
            ko_keywords_freq = [kor_freq_counter(f, temp_ko_text) for f in ko_keywords]

        if efect_teaser == None:
            ko_keywords_freq = [kor_freq_counter(f, title_kr) for f in ko_keywords]
        else:
            temp_ko_text += "\n" + efect_teaser
            ko_keywords_freq = [kor_freq_counter(f, temp_ko_text) for f in ko_keywords]
        

        eng_keywords_freq = [eng_freq_counter(f, title_en) for f in eng_keywords]
        # 한국어 키워드:빈도 사전
        key_dict_ko = {i:j for i, j in zip(ko_keywords, ko_keywords_freq)}
        # 영어 키워드:빈도 사전
        key_dict_en = {i:j for i, j in zip(eng_keywords, eng_keywords_freq)}
        # 합치기
        key_dict_total = key_dict_ko
        for k in key_dict_en:
            if k not in key_dict_total:
                key_dict_total[k] = key_dict_en[k]
            else:
                key_dict_total[k] = key_dict_total[k] + key_dict_en[k]
        # 사전들 배열하고 0의 빈도 키워드 빼기
        key_dict_ko = dict(sorted(key_dict_ko.items(), key=lambda item:item[1], reverse=True))
        key_dict_ko = {key:key_dict_ko[key] for key in key_dict_ko if key_dict_ko[key] > 0}
        key_dict_en = dict(sorted(key_dict_en.items(), key=lambda item:item[1], reverse=True))
        key_dict_en = {key:key_dict_en[key] for key in key_dict_en if key_dict_en[key] > 0}
        key_dict_total = dict(sorted(key_dict_total.items(), key=lambda item:item[1], reverse=True))
        key_dict_total = {key:key_dict_total[key] for key in key_dict_total if key_dict_total[key] > 0}
        
        # print(eng_keywords_freq)
        print(f"KEYWORD LEN: {len(eng_keywords_freq)}")
        if len(eng_keywords_freq) < EXPECTED_KEYWORDS_NUMBER:
            temp_text = ""
            if title_en == None:
                title_en = ""
            if keywords_en != None:
                temp_text += keywords_en
            key_dict_en = complement_with_rake(title_en, temp_text, key_dict_en, rake)
            # print(keywords_dict)
            # print(len(keywords_dict)
        print(f"KEYWORD LEN: {len(eng_keywords_freq)}")

        for k in key_dict_ko:
            ko_res_no.append(r[0])
            ko_keyword.append(k)
            ko_freq.append(key_dict_ko[k])
        
        for k in key_dict_en:
            en_res_no.append(r[0])
            en_keyword.append(k)
            en_freq.append(key_dict_en[k])
        
        for k in key_dict_ko:
            total_res_no.append(r[0])
            total_keyword.append(k)
            total_freq.append(key_dict_total[k])
        # 사전별 데이터 프레임 만들기
        ko_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
        ko_df["RES_NO"] = ko_res_no
        ko_df["KEYWORD"] = ko_keyword
        ko_df["FREQ"] = ko_freq
        en_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
        en_df["RES_NO"] = en_res_no
        en_df["KEYWORD"] = en_keyword
        en_df["FREQ"] = en_freq
        total_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
        total_df["RES_NO"] = total_res_no
        total_df["KEYWORD"] = total_keyword
        total_df["FREQ"] = total_freq
        
        count += 1
        if count >= 100:
            count = 0
            if not os.path.exists(os.path.join(OUTPUT_FOLDER, "KOR")):
                os.makedirs(os.path.join(OUTPUT_FOLDER, "KOR"))
            if not os.path.exists(os.path.join(OUTPUT_FOLDER, "ENG")):
                os.makedirs(os.path.join(OUTPUT_FOLDER, "ENG"))
            if not os.path.exists(os.path.join(OUTPUT_FOLDER, "TOTAL")):
                os.makedirs(os.path.join(OUTPUT_FOLDER, "TOTAL"))

            # 엑셀 파일 내보내기
            ko_df.to_excel(os.path.join(OUTPUT_FOLDER, "KOR", "project_keywords_kor.xlsx"), index=False)
            en_df.to_excel(os.path.join(OUTPUT_FOLDER, "ENG", "project_keywords_eng.xlsx"), index=False)
            total_df.to_excel(os.path.join(OUTPUT_FOLDER, "TOTAL", "project_keywords_total.xlsx"), index=False)
    
    # 사전별 데이터 프레임 만들기
    ko_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
    ko_df["RES_NO"] = ko_res_no
    ko_df["KEYWORD"] = ko_keyword
    ko_df["FREQ"] = ko_freq
    en_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
    en_df["RES_NO"] = en_res_no
    en_df["KEYWORD"] = en_keyword
    en_df["FREQ"] = en_freq
    total_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
    total_df["RES_NO"] = total_res_no
    total_df["KEYWORD"] = total_keyword
    total_df["FREQ"] = total_freq

    if not os.path.exists(os.path.join(OUTPUT_FOLDER, "KOR")):
        os.makedirs(os.path.join(OUTPUT_FOLDER, "KOR"))
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, "ENG")):
        os.makedirs(os.path.join(OUTPUT_FOLDER, "ENG"))
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, "TOTAL")):
        os.makedirs(os.path.join(OUTPUT_FOLDER, "TOTAL"))

    # 엑셀 파일 내보내기
    ko_df.to_excel(os.path.join(OUTPUT_FOLDER, "KOR", "project_keywords_kor.xlsx"), index=False)
    en_df.to_excel(os.path.join(OUTPUT_FOLDER, "ENG", "project_keywords_eng.xlsx"), index=False)
    total_df.to_excel(os.path.join(OUTPUT_FOLDER, "TOTAL", "project_keywords_total.xlsx"), index=False)

    return


if __name__ == "__main__":
    investigation_task_keywords(checkpoint=True)