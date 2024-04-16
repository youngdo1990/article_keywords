# -*- coding: utf-8 -*-

from db_connect import *
import os
from tqdm import tqdm

import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import pandas as pd
from konlpy.tag import Kkma
from tqdm.contrib import tzip
from langdetect import detect

nltk.download("wordnet")
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')


def is_number(a):
    if isinstance(a, int):
        return True
    if isinstance(a, float):
        return True
    return False


def get_corpus_from_patent(result):
    corpus_words = []
    for r in tqdm(result):
        if r[1]:
            corpus_words.append(r[1])
    return corpus_words


def multilingua_pos(corpus_words):
    
    kkma = Kkma()
    pos_list = []
    for word in corpus_words:
        
        if not is_number(word):
            try:
                if detect(word)=="ko":
                    pos_list.append(kkma.pos(word))
                elif detect(word) == "ja":
                    pass
                else:
                    pos_list.append(nltk.pos_tag([word])[0])
            except:
                pass
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
        if not is_number(original):
            try:
                lang = detect(original)
                if lang == "ko":
                    lemma_dict[original] = standarize_korean_words(word)[0]
                else:
                    lemma_dict[original] = lemmatizer.lemmatize(word[0])
                        # print(f"{original}==>{lemmatizer.lemmatize(word[0])}")
            except:
                pass
            
        # else:
        #     # 한국어 단어 LEMMATIZATION
        
        # print(f"{original} ---> {word}")
    # for l in lemma_dict:
    #     print(f"{l}==>{lemma_dict[l]}")
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


def eliminate_no_meaning_tokens(df, eliminated_words, eliminate_words_eng):
    stopwords_file = os.path.join("..", "kr_stopwords", "stopwords_full.txt")
    kr_stopwords = []
    eng_stopwords = stopwords.words("english")
    with open(stopwords_file, "r", encoding="utf-8", errors="ignore") as file:
        for l in file:
            line = l.strip()
            kr_stopwords.append(line)
    # 의미 없는 키워드 제거
    for w in eliminated_words:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    for w in eliminate_words_eng:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    for w in stopwords_file:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    for w in eng_stopwords:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    return df


def replace_lemmas(df, lemma2update):
    return df.replace(lemma2update)


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
            # print(df["KEYWORD"][i])
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


def tuples2df(data):
    df = pd.DataFrame(list(data), columns=["MST_ID", "KEYWORD", "FREQ"])
    return df


def postprocess_domestic(domestic_df):
    corpus_words = get_corpus_from_patent(domestic_df)
    corpus_words = remove_repeated(corpus_words)
    original_words = corpus_words
    corpus_words = multilingua_pos(corpus_words)
    # print(f"CORPUS WORDS>>\n{corpus_words}")
    lemma_dict = make_lemma_dict(original_words, corpus_words)
    domestic_df = tuples2df(domestic_df)
    eliminate_words_eng, eliminate_words_kor, corpus_words_hat = make_eliminate_list(original_words, corpus_words)
    domestic_df = eliminate_no_meaning_tokens(domestic_df, eliminate_words_kor, eliminate_words_eng)
    lemma2update = {k:lemma_dict[k] for k in lemma_dict if k!=lemma_dict[k]}
    domestic_df = replace_lemmas(domestic_df, lemma2update)
    domestic_df = freq_recalculate(domestic_df)
    return domestic_df


def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


if __name__ == "__main__":
    df = pd.read_excel("DOMESTIC_TESTING_DF.xlsx")
    df = get_dataframes_tuple(df)
    df = postprocess_domestic(df)
    df.to_excel("DOMESTIC[OUTPUT].xlsx", index=False)