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
    word_lang = []
    for r in tqdm(result):
        if r[1]:
            corpus_words.append(r[1])
            word_lang.append("en")
        else:
            corpus_words.append(r[2])
            word_lang.append("ko")
    conn.close()

    # corpus_words = [nltk.pos_tag([w])[0] for w in corpus_words]
    return corpus_words, word_lang


def multilingua_pos(corpus_words, word_lang):
    kkma = Kkma()
    pos_list = []
    for word, l in tqdm(zip(corpus_words, word_lang)):
        if l=="ko":
            pos_list.append(kkma.pos(word))
        else:
            pos_list.append(nltk.pos_tag([word])[0])
    return pos_list


def remove_repeated(corpus_words, word_lang):
    new_corpus_words, new_word_lang = [], []
    for word, lang in zip(corpus_words, word_lang):
        if word not in new_corpus_words:
            new_corpus_words.append(word)
            new_word_lang.append(lang)
    return new_corpus_words, new_word_lang


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


def make_lemma_dict(original_words, corpus_words, word_lang):
    ## Lemmatizer (ENGLISH)
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    plural_tags_eng = ["NNS", "NNPS"]
    ## KOREAN Lemmatizer -> standarize_korean_words 함수 [알바로 직접으로 만드는 것이다]
    lemma_dict = {}

    for original, word, lang in tqdm(zip(original_words, corpus_words, word_lang)):
        if lang == "en":
            # 영어 단어 LEMMATIZATION
            if word[1] not in plural_tags_eng:
                lemma_dict[original] = word[0]
            else:
                lemma_dict[original] = lemmatizer.lemmatize(word[0])
        else:
            # 한국어 단어 LEMMATIZATION
            lemma_dict[original] = standarize_korean_words(word)[0]
    return lemma_dict


def make_eliminate_list(original_words, corpus_words, word_lang):
    allowed_korean_tags = ["NNG", "NNP", "VV"]
    unallowed_tags_en = ["JJR", "JJS", "RB", "RBR", "RBS", "CD"]
    
    eliminate_words_eng = []
    eliminate_words_kor = []
    corpus_words_hat = []
    word_lang_hat = []
    for original, word, lang in zip(original_words, corpus_words, word_lang):
        if lang == "en":
            if word[1] not in unallowed_tags_en:
                corpus_words_hat.append(original)
                word_lang_hat.append(lang)
            else:
                eliminate_words_eng.append(original)
        else:
            # 한국어 처리
            if len(word) > 1:
                if word[0][1] in allowed_korean_tags:
                    corpus_words_hat.append(word[0][0])
                    word_lang_hat.append(lang)
                else:
                    eliminate_words_kor.append(word[0][0])  
            else:
                if word[0][1] not in allowed_korean_tags:
                    eliminate_words_kor.append(word[0][0])
                else:
                    corpus_words_hat.append(word[0][0])
                    word_lang_hat.append(lang)
    return eliminate_words_eng, eliminate_words_kor, corpus_words_hat, word_lang_hat


def eliminate_no_meaning_tokens(table_name, eliminated_words_eng, eliminated_words_kor):
    # 의미 없는 키워드 제거
    conn = db_connection()
    cur = conn.cursor()

    print("숫자, 형용사, 부사 삭제 중입니다")
    for w in tqdm(eliminated_words_eng):
        query = "DELETE FROM " + table_name + " WHERE KEYWORD_EN='" + w + "'"
        #print(query)
        cur.execute(query)
        result = cur.fetchall()
        conn.commit()
    for w in tqdm(eliminated_words_kor):
        query = "DELETE FROM " + table_name + " WHERE KEYWORD_KR='" + w + "'"
        #print(query)
        cur.execute(query)
        result = cur.fetchall()
        conn.commit()
    conn.close()
    return


if __name__ == "__main__":
    corpus_words, word_lang = get_corpus_from_project("FCT_PATENT_OVERSEAS_KEYWORDS")
    # print(corpus_words)
    corpus_words, word_lang = remove_repeated(corpus_words, word_lang)
    original_words = corpus_words
    corpus_words = multilingua_pos(corpus_words, word_lang)
    lemma_dict = make_lemma_dict(original_words, corpus_words, word_lang)
    eliminate_words_eng, eliminate_words_kor, corpus_words_hat, word_lang_hat = make_eliminate_list(original_words, corpus_words, word_lang)
    eliminate_no_meaning_tokens("FCT_PATENT_OVERSEAS_KEYWORDS", eliminate_words_eng, eliminate_words_kor)

