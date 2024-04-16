# -*- coding: utf-8 -*-

from db_connect import *
import os
from tqdm import tqdm

import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
import pandas as pd
from konlpy.tag import Kkma
from tqdm.contrib import tzip



def get_corpus_from_project(table_name):
    
    conn = db_connection()
    cur = conn.cursor()
    # 콜럼 이름
    query = "DESC " + table_name
    cur.execute(query)
    result = cur.fetchall()
    
    # 컬럼 이름
    query =  "SELECT * FROM " + table_name
    cur.execute(query)
    result = cur.fetchall()
    corpus_words = []
    # word_lang = []
    # for r in tqdm(result):
    #     if r[1]:
    #         corpus_words.append(r[1])
    #         word_lang.append("en")
    #     else:
    #         corpus_words.append(r[2])
    #         word_lang.append("ko")
    conn.close()

    # corpus_words = [nltk.pos_tag([w])[0] for w in corpus_words]
    return corpus_words


def multilingua_pos(corpus_words):
    kkma = Kkma()
    pos_list = []
    for word in corpus_words:
        pos_list.append(nltk.pos_tag([word])[0])
    return pos_list


def remove_repeated(corpus_words):
    new_corpus_words = []
    for word in corpus_words:
        if word not in new_corpus_words:
            new_corpus_words.append(word)
            
    return new_corpus_words


def standarize_korean_words(pos):
    allowed_korean_tags = ["NNG", "NNP", "VV"]
    
    if (pos[0][1] == "NNG" or pos[0][1] == "NNP") and (pos[-1][1] == "NNG" or pos[-1][1] == "NNP"):
            "".join([p[0] for p in pos]), pos[0][1]
    if pos[0][1] in allowed_korean_tags:
        word_list = [p[0] for p in pos]
        tag_list = [p[1] for p in pos]
        if (tag_list[0] == "NNG" or tag_list[0] == "NNP") and (len(set(tag_list))  == 1):
            return "".join(word_list), tag_list[0]
        temp_word = []
        
        for i in reversed(range(len(word_list))):
            if tag_list[i] in allowed_korean_tags or tag_list[i] == "XSV":
                 temp_word = word_list[:i+1]
                 tag_list = tag_list[:i+1]
                #  sub_word = word_list[i+1:]
                 break
        if tag_list[-1] == "XSV":
            temp_word.append("다")
        return "".join(temp_word), tag_list[-1]
    
    return "".join([p[0] for p in pos]), pos[0][1]


def make_lemma_dict(original_words, corpus_words):
    ## Lemmatizer (ENGLISH)
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    plural_tags_eng = ["NNS", "NNPS"]
    ## KOREAN Lemmatizer -> standarize_korean_words 함수 [알바로 직접으로 만드는 것이다]
    lemma_dict = {}

    for original, word in tqdm(zip(original_words, corpus_words)):
        # if lang == "en":
            # 영어 단어 LEMMATIZATION
            #if word[1] not in plural_tags_eng:
        lemma_dict[original] = word[0]
        #     else:
        #         lemma_dict[original] = lemmatizer.lemmatize(word[0])
        # else:
        #     # 한국어 단어 LEMMATIZATION
        lemma_dict[original] = standarize_korean_words(word)[0]
    return lemma_dict


def make_eliminate_list(original_words, corpus_words):
    allowed_korean_tags = ["NNG", "NNP", "VV"]
    unallowed_tags_en = ["JJR", "JJS", "RB", "RBR", "RBS", "CD"]
    
    eliminate_words_eng = []
    eliminate_words_kor = []
    corpus_words_hat = []
    for original, word in zip(original_words, corpus_words):
        # if lang == "en":
        #     if word[1] not in unallowed_tags_en:
        #         corpus_words_hat.append(original)
        #         word_lang_hat.append(lang)
        #     else:
        #         eliminate_words_eng.append(original)
        # else:
            # 한국어 처리
        if len(word) > 1:
            if word[0][1] in allowed_korean_tags:
                corpus_words_hat.append(word[0][0])
                # word_lang_hat.append(lang)
            else:
                eliminate_words_kor.append(word[0][0])  
        else:
            if word[0][1] not in allowed_korean_tags:
                eliminate_words_kor.append(word[0][0])
            else:
                corpus_words_hat.append(word[0][0])
                # word_lang_hat.append(lang)
    return eliminate_words_eng, eliminate_words_kor, corpus_words_hat


def eliminate_no_meaning_tokens(table_name, eliminated_words_eng, eliminated_words_kor):
    # 의미 없는 키워드 제거
    conn = db_connection()
    cur = conn.cursor()

    print("숫자, 형용사, 부사 삭제 중입니다")
    for w in tqdm(eliminated_words_eng):
        query = "DELETE FROM " + table_name + " WHERE KEYWORD='" + w + "'"
        #print(query)
        cur.execute(query)
        result = cur.fetchall()
        conn.commit()
    
    conn.close()
    return


def replace_lemma(df, non_lemma, non_lemma_lang):
    for word, lang in tzip(non_lemma, non_lemma_lang):
        df["KEYWORD"] = df["KEYWORD"].replace(word, lemma_dict[word])
    return df


def freq_recalculate(df):
    doc_keywords = {}
    for i in tqdm(df.index):
        if df["MST_ID"][i] not in doc_keywords:
                doc_keywords[df["MST_ID"][i]] = {}
        if "KEYWORD" not in doc_keywords[df["MST_ID"][i]]:
            doc_keywords[df["MST_ID"][i]]["KEYWORD"] = {}
        
        # 영어
        # if df["KEYWORD"][i]:
        if df["KEYWORD"][i] not in doc_keywords[df["MST_ID"][i]]["KEYWORD"]:
            print(df["KEYWORD"][i])
            doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]] = {}
            # print(doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]])
            doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]]["FREQ"] = df["FREQ"][i]
        # else:
        # doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]] = {}
            #     doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]]["LANG"] = "en"
            # 빈도 다시 게산 한국어
        doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]]["KEYWORD"] = df["KEYWORD"][i]
        doc_keywords[df["MST_ID"][i]]["KEYWORD"][df["KEYWORD"][i]]["FREQ"] += df["FREQ"][i]
    
    columns = list(df.columns)
    ## 업데이트된 빈도
    updated_table = {"MST_ID": [],
                    "KEYWORD": [],
                    "FREQ": []
                    }
    ## 업데이트된 빈도
    print(doc_keywords)
    for doc in tqdm(doc_keywords):
        freq = {}
        for k in doc_keywords[doc]["KEYWORD"]:
            freq[k] = doc_keywords[doc]["KEYWORD"][k]["FREQ"]
        freq = dict(sorted(freq.items(), key=lambda item:item[1], reverse=True))
        
        for f in freq:
            updated_table["MST_ID"].append(doc)
            updated_table["KEYWORD"].append(f)
            updated_table["FREQ"].append(freq[f])

    new_df = pd.DataFrame(updated_table)

    return new_df


def lemma_freq_update(table_name, corpus_words, lemma_dict):
    non_lemma, non_lemma_lang = [], []
    for word in corpus_words:
        if word != lemma_dict[word]:
            non_lemma.append(word)
            # non_lemma_lang.append(lang)
    # CONVERT TABLE INTO DATAFRAME
    conn = db_connection()
    query = "SELECT * FROM " + table_name
    df = pd.read_sql(query, conn)
    # CHANGE ALL WORDS FOR ITS LEMMA
    print("토큰 LEMMA로 변걍 중입니다")
    df = replace_lemma(df, non_lemma, non_lemma_lang)
    # RECALCULATE FREQUENCIES
    print("빈도 재계산 및 리스트 배열")
    df = freq_recalculate(df)    
    return df


BATCH_SIZE = 10
import numpy as np

def update_database(df, table_name):
    conn = db_connection()
    cur = conn.cursor()
    columns = list(df.columns)
    queries = []
    insert_base = "INSERT INTO " + table_name + " ("
    for col in columns:
        insert_base += col + ", "
    insert_base = insert_base[:-2]
    insert_base += ") VALUES "
        
    count = 0

    # df = df.iloc[:100]

    insert = insert_base
    for i in df.index:
        for n, col in enumerate(columns):
            if n == 0:
                insert += "("
            insert += "'" + str(df[col][i]) + "', "
        insert += insert[:-2]
        insert += "), "
        count += 1
        if count == BATCH_SIZE:
            insert += ")"
            with open("log0711.txt", "+a", encoding="utf-8", errors="ignore") as file:
                file.write(insert)
            #insert = insert[:-1]
            queries.append(insert)
            insert = insert_base
            count = 0
            
    
    conn.close()


if __name__ == "__main__":
    corpus_words = get_corpus_from_project("FCT_PATENT_DOMESTIC_KEYWORDS")
    # print(corpus_words)
    corpus_words = remove_repeated(corpus_words)
    original_words = corpus_words
    corpus_words = multilingua_pos(corpus_words)
    lemma_dict = make_lemma_dict(original_words, corpus_words)
    eliminate_words_eng, eliminate_words_kor, corpus_words_hat = make_eliminate_list(original_words, corpus_words)
    #eliminate_no_meaning_tokens("FCT_PROJECT_KEYWORDS", eliminate_words_eng, eliminate_words_kor)
    df = lemma_freq_update("FCT_PATENT_DOMESTIC_KEYWORDS", original_words, lemma_dict)
    df.to_excel("patent_domestic_clean.xlsx", index=False)
    # update_database(df, "FCT_PROJECT_KEYWORDS")
