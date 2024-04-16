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
    for r in tqdm(result):
        corpus_words.append(r[1])
    conn.close()

    # corpus_words = [nltk.pos_tag([w])[0] for w in corpus_words]
    return corpus_words


def korean_pos(corpus_words):
    kkma = Kkma()
    pos_list = []
    for word in tqdm(corpus_words):
        pos_list.append(kkma.pos(word))
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
        # 한국어 단어 LEMMATIZATION
        lemma_dict[original] = standarize_korean_words(word)[0]
    return lemma_dict


def make_eliminate_list(original_words, corpus_words):
    allowed_korean_tags = ["NNG", "NNP", "VV"]
    unallowed_tags_en = ["JJR", "JJS", "RB", "RBR", "RBS", "CD"]
    
    eliminate_words_eng = []
    eliminate_words_kor = []
    corpus_words_hat = []
    for original, word in zip(original_words, corpus_words):
        # 한국어 처리
        if len(word) > 1:
            if word[0][1] in allowed_korean_tags:
                corpus_words_hat.append(word[0][0])
            else:
                eliminate_words_kor.append(word[0][0])  
        else:
            if word[0][1] not in allowed_korean_tags:
                eliminate_words_kor.append(word[0][0])
            else:
                corpus_words_hat.append(word[0][0])
    return eliminate_words_eng, corpus_words_hat


def eliminate_no_meaning_tokens(table_name, eliminated_words_kor):
    # 의미 없는 키워드 제거
    conn = db_connection()
    cur = conn.cursor()

    print("숫자, 형용사, 부사 삭제 중입니다")
    for w in tqdm(eliminated_words_kor):
        query = "DELETE FROM " + table_name + " WHERE KEYWORD='" + w + "'"
        #print(query)
        cur.execute(query)
        result = cur.fetchall()
        conn.commit()
    conn.close()
    return


if __name__ == "__main__":
    corpus_words = get_corpus_from_project("FCT_PATENT_DOMESTIC_KEYWORDS")
    print(corpus_words)
    corpus_words = remove_repeated(corpus_words)
    original_words = corpus_words
    corpus_words = korean_pos(corpus_words)
    lemma_dict = make_lemma_dict(original_words, corpus_words)
    eliminate_words_kor, corpus_words_hat = make_eliminate_list(original_words, corpus_words)
    eliminate_no_meaning_tokens("FCT_PATENT_DOMESTIC_KEYWORDS", eliminate_words_kor)

