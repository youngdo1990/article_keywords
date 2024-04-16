# -*- coding: utf-8 -*-
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
import os
from langdetect import detect
from nltk.corpus import stopwords
nltk.download('stopwords')

def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


def get_corpus_from_project(result):
    corpus_words = []
    word_lang = []
    for r in result:
        try:
            if detect(r[1]) == "ko":
                word_lang.append("ko")
            else:
                word_lang.append("en")
            corpus_words.append(r[1])
        except:
            pass
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


def eliminate_no_meaning_tokens(df, eliminated_words_eng, eliminated_words_kor):
    stopwords_file = os.path.join("kr_stopwords", "stopwords_full.txt")
    # stopwords_file = os.path.join("..", "kr_stopwords", "stopwords_full.txt")
    kr_stopwords = []
    eng_stopwords = stopwords.words("english")
    with open(stopwords_file, "r", encoding="utf-8", errors="ignore") as file:
        for l in file:
            line = l.strip()
            kr_stopwords.append(line.strip())
    # 의미 없는 키워드 제거
    print("숫자, 형용사, 부사 삭제 중입니다")
    for w in tqdm(eliminated_words_eng):
        df.drop(df[df["KEYWORD"]==w].index, inplace=True)
    for w in tqdm(eliminated_words_kor):
        df.drop(df[df["KEYWORD"]==w].index, inplace=True)
    for w in kr_stopwords:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    for w in eng_stopwords:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    return df


def replace_lemma(df, non_lemma, non_lemma_lang, lemma_dict):
    for word, lang in tzip(non_lemma, non_lemma_lang):
        df["KEYWORD"] = df["KEYWORD"].replace(lemma_dict[word], word)
    return df


def freq_recalculate(df):
    doc_keywords = {}
    for i in tqdm(df.index):
        if df["RES_NO"][i] not in doc_keywords:
                doc_keywords[df["RES_NO"][i]] = {}
        if "KEYWORD" not in doc_keywords[df["RES_NO"][i]]:
            doc_keywords[df["RES_NO"][i]]["KEYWORD"] = {}
        if df["KEYWORD_KR"][i]:
            # 한국어
            if df["KEYWORD_KR"][i] not in doc_keywords[df["RES_NO"][i]]["KEYWORD"]:
                doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]] = {}
                doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]]["LANG"] = "ko"
                doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]]["FREQ"] = df["FREQ"][i]
            else:
                #doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]] = {}
                doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]]["LANG"] = "ko"
                # 빈도 다시 게산 한국어
                doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]]["FREQ"] += df["FREQ"][i]
        else:
            # 영어
            if df["KEYWORD_EN"][i]:
                if df["KEYWORD_EN"][i] not in doc_keywords[df["RES_NO"][i]]["KEYWORD"]:
                    doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_EN"][i]] = {}
                    doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_EN"][i]]["LANG"] = "en"
                    doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_EN"][i]]["FREQ"] = df["FREQ"][i]
                else:
                    # doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_KR"][i]] = {}
                    doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_EN"][i]]["LANG"] = "en"
                    # 빈도 다시 게산 한국어
                    doc_keywords[df["RES_NO"][i]]["KEYWORD"][df["KEYWORD_EN"][i]]["FREQ"] += df["FREQ"][i]
        
    columns = list(df.columns)
    ## 업데이트된 빈도
    updated_table = {"RES_NO": [],
                    "KEYWORD_EN": [],
                    "KEYWORD_KR": [],
                    "FREQ": []
                    }
    ## 업데이트된 빈도
    for doc in tqdm(doc_keywords):
        freq, lang = {}, {}
        for k in doc_keywords[doc]["KEYWORD"]:
            freq[k] = doc_keywords[doc]["KEYWORD"][k]["FREQ"]
            lang[k] = doc_keywords[doc]["KEYWORD"][k]["LANG"]
        freq = dict(sorted(freq.items(), key=lambda item:item[1], reverse=True))
        
        for f in freq:
            updated_table["RES_NO"].append(doc)
            if lang[f] == "ko":
                updated_table["KEYWORD_EN"].append("")
                updated_table["KEYWORD_KR"].append(f)
            elif lang[f] == "en":
                updated_table["KEYWORD_EN"].append(f)
                updated_table["KEYWORD_KR"].append("")
            updated_table["FREQ"].append(freq[f])

    new_df = pd.DataFrame(updated_table)

    return new_df


def tuples2df(data):
    df = pd.DataFrame(list(data), columns=["RES_NO", "KEYWORD", "FREQ"])
    print(df)
    return df


def lemma_freq_update(df, corpus_words, word_lang, lemma_dict):
    non_lemma, non_lemma_lang = [], []
    # for word, lang in zip(corpus_words, word_lang):
    #     if word != lemma_dict[word]:
    #         non_lemma.append(word)
    #         non_lemma_lang.append(lang)
    # CHANGE ALL WORDS FOR ITS LEMMA
    print("토큰 LEMMA로 변걍 중입니다")
    # df = replace_lemma(df, non_lemma, non_lemma_lang, lemma_dict)
    # RECALCULATE FREQUENCIES
    print("빈도 재계산 및 리스트 배열")
    df = freq_recalculate(df)    
    return df


def adjust_table(df):
    kkma = Kkma()
    new_df = pd.DataFrame(columns=["RES_NO", "KEYWORD_EN", "KEYWORD_KR", "FREQ"])
    res_no = []
    keyword_en = []
    keyword_kr = []
    freq = []
    for i in df.index:
        try:
            if detect(df["KEYWORD"][i]) == "ko":
                res_no.append(df["RES_NO"][i])
                keyword_kr.append(df["KEYWORD"][i])
                keyword_en.append("")
                freq.append(df["FREQ"][i])
            else:
                pos = kkma.pos(df['KEYWORD'][i])
                if len(pos) <= 1:
                    res_no.append(df["RES_NO"][i])
                    keyword_kr.append("")
                    keyword_en.append(df["KEYWORD"][i])
                    freq.append(df["FREQ"][i])
        except:
            pass
    new_df["RES_NO"] = res_no
    new_df["KEYWORD_EN"] = keyword_en
    new_df["KEYWORD_KR"] = keyword_kr
    new_df["FREQ"] = freq
    return new_df


def treat_mixed(corpus_words):
    allowed_korean_tags = ["NNG", "NNP", "VV"]
    new_corpus_words = []
    kkma =Kkma()
    for word in corpus_words:
        l = detect(word)
        if l == "ko":
            new_corpus_words.append(word)
        else:
            pos = kkma.pos(word)
            for p in pos:
                if p[1] == "OL":
                    new_corpus_words.append(p[0])
                elif p[1] in allowed_korean_tags:
                    new_corpus_words.append(p[0])
    return new_corpus_words


def replace_lemmas(df, lemma2update):
    new_df = pd.DataFrame(columns=["RES_NO", "KEYWORD", "FREQ"])
    res_no = []
    keyword = []
    freq = []
    for i in df.index:
        res_no.append(df["RES_NO"][i])
        if df["KEYWORD"][i] in lemma2update:
            keyword.append(lemma2update[df["KEYWORD"][i]])
        else:
            keyword.append(df["KEYWORD"][i])
        freq.append(df["FREQ"][i])
    new_df["RES_NO"] = res_no
    new_df["KEYWORD"] = keyword
    new_df["FREQ"] = freq  
    return new_df


def postprocess_project(project_df):
    project_df = get_dataframes_tuple(project_df)
    corpus_words, word_lang = get_corpus_from_project(project_df)
    # corpus_words, word_lang = treat_mixed(corpus_words, word_lang)
    corpus_words, word_lang = remove_repeated(corpus_words, word_lang)
    original_words = corpus_words
    corpus_words = multilingua_pos(corpus_words, word_lang)
    lemma_dict = make_lemma_dict(original_words, corpus_words, word_lang)
    lemma2update = {k:lemma_dict[k] for k in lemma_dict if k!=lemma_dict[k]}
    project_df = tuples2df(project_df)
    eliminate_words_eng, eliminate_words_kor, corpus_words_hat, word_lang_hat = make_eliminate_list(original_words, corpus_words, word_lang)
    project_df = eliminate_no_meaning_tokens(project_df, eliminate_words_eng, eliminate_words_kor)
    project_df = replace_lemmas(project_df, lemma2update)
    project_df = adjust_table(project_df)
    project_df = freq_recalculate(project_df)
    # project_df.to_excel("project_clean.xlsx", index=False)
    return project_df


if __name__ == "__main__":
    df = pd.read_excel("PROJECT_TESTDATA.xlsx")
    project_df = postprocess_project(df)
    project_df.to_excel("PROJECT[OUTPUT].xlsx", index=False)