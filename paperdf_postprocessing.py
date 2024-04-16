# -*- coding: utf-8 -*-

import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
import pandas as pd
from tqdm import tqdm
from db_connect import *
import os

import itertools

PAPER_FILE_BASE = "FCT_PAPER_KEYWORDS"


def make_df():
    file_list = os.listdir()
    file_list = [f for f in file_list if f.endswith(".xlsx")]
    file_list = [f for f in file_list if f.startswith(PAPER_FILE_BASE)]
    df_list = []
    for file in file_list:
        df_list.append(pd.read_excel(file))
    # df = pd.concat(df_list)
    df = df_list[0]
    corpus_words = []

    pmid = df_list[0]["PMID"].to_list()
    cn =  df_list[0]["CN"].to_list()
    keyword = df_list[0]["KEYWORD"].to_list()
    freq = df_list[0]["FREQ"].to_list()

    for i in range(1, len(df_list)):
        pmid.extend(df_list[i]["PMID"].to_list())
        cn.extend(df_list[i]["CN"].to_list())
        keyword.extend(df_list[i]["KEYWORD"].to_list())
        freq.extend(df_list[i]["FREQ"].to_list())

    complete_df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])
    complete_df["PMID"] = pmid
    complete_df["CN"] = cn
    complete_df["KEYWORD"] = keyword
    complete_df["FREQ"] = freq
    complete_df = complete_df.fillna("")
    return complete_df


def get_corpus_from_paper(df):
    corpus_words = []
    for i in tqdm(df.index):
        if df["KEYWORD"][i] not in corpus_words:
            corpus_words.append(df["KEYWORD"][i])

    corpus_words = [nltk.pos_tag([w])[0] for w in corpus_words]
    return corpus_words


def eliminate_no_meaning_tokens(df, eliminated_words):
    # 의미 없는 키워드 제거
    for w in eliminated_words:
        df.drop(df[df["KEYWORD"] == w].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "year"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "month"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "week"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "day"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "hour"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "minute"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "hr"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "min"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "second"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "sec"].index, inplace=True)
    df.drop(df[df["KEYWORD"] == "sec"].index, inplace=True)
    pmid = []
    cn = []
    keyword = []
    freq = []
    print("PAPER KEYWORDS")
    for i in df.index:
        if len(df["KEYWORD"][i]) > 1:
            pmid.append(df["PMID"][i])
            cn.append(df["CN"][i])
            keyword.append(df["KEYWORD"][i])
            freq.append(df["FREQ"][i])
    new_df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])
    new_df["PMID"] = pmid
    new_df["CN"] = cn
    new_df["KEYWORD"] = keyword
    new_df["FREQ"] = freq
    return new_df


def freq_recalculate(df):
    print("빈도 업데이트")
    pmid = {}
    cn = {}
    for i in df.index:
        if df["PMID"][i] != "":
            if df["PMID"][i] not in pmid:
                pmid[df["PMID"][i]] = {}
                if df["KEYWORD"][i] not in pmid[df["PMID"][i]]:
                    pmid[df["PMID"][i]][df["KEYWORD"][i]] = df["FREQ"][i]
                else:
                    pmid[df["PMID"][i]][df["KEYWORD"][i]] += df["FREQ"][i]
            else:
                if df["KEYWORD"][i] not in pmid[df["PMID"][i]]:
                    pmid[df["PMID"][i]][df["KEYWORD"][i]] = df["FREQ"][i]
                else:
                    pmid[df["PMID"][i]][df["KEYWORD"][i]] += df["FREQ"][i]
        if df["CN"][i] != "":
            if df["CN"][i] not in cn:
                cn[df["CN"][i]] = {}
                if df["KEYWORD"][i] not in cn[df["CN"][i]]:
                    cn[df["CN"][i]][df["KEYWORD"][i]] = df["FREQ"][i]
                else:
                    cn[df["CN"][i]][df["KEYWORD"][i]] += df["FREQ"][i]
            else:
                if df["KEYWORD"][i] not in cn[df["CN"][i]]:
                    cn[df["CN"][i]][df["KEYWORD"][i]] = df["FREQ"][i]
                else:
                    cn[df["CN"][i]][df["KEYWORD"][i]] += df["FREQ"][i]
    
    for id in pmid:
        pmid[id] = dict(sorted(pmid[id].items(), key=lambda item:item[1], reverse=True))
    for id in cn:
        cn[id] = dict(sorted(cn[id].items(), key=lambda item:item[1], reverse=True))

    pmid_hat = []
    id2 = []
    keyword = []
    freq = []

    for id in pmid:
        for row in pmid[id]:
            pmid_hat.append(id)
            id2.append("")
            keyword.append(row)
            freq.append(pmid[id][row])
    for id in cn:
        for row in cn[id]:
            pmid_hat.append("")
            id2.append(id)
            keyword.append(row)
            freq.append(cn[id][row])

    new_df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])
    new_df["PMID"] = pmid_hat
    new_df["CN"] = id2
    new_df["KEYWORD"] = keyword
    new_df["FREQ"] = freq
    new_df.to_excel("FREQ_TEST.xlsx", index=False)
    return new_df


def replace_lemmas(df, lemma2update):
    for word in lemma2update:
        df["KEYWORD"] = df["KEYWORD"].replace(word, lemma2update[word])
    df = freq_recalculate(df)
    return df


def make_paper_clean_file(df):
    DF_MAX_SIZE = 500000
    PAPER_KEYWORDS = "paper_keywords(clean)"

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


def postprocess_paper_df(df):
    ## Lemmatizer
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    corpus_words = get_corpus_from_paper(df)
    unallowed_tags = ["JJ", "JJR", "JJS", "RB", "RBR", "RBS", "CD"]
    eliminated_words = [c for c in corpus_words if c[1] in unallowed_tags]
    corpus_words = [c for c in corpus_words if c[1] not in unallowed_tags]
    plural_tags = ["NNS", "NNPS"]
    lemma_dict = {}
    for w in tqdm(corpus_words):
        if w[1] not in plural_tags:
            lemma_dict[w[0]] = w[0]
        else:
            lemma_dict[w[0]] = lemmatizer.lemmatize(w[0])
    eliminated_words = [w[0] for w in eliminated_words]
    lemma2update = {k:lemma_dict[k] for k in lemma_dict if k!=lemma_dict[k]}
    df.to_excel("TEST(8_2).xlsx", index=False)
    df = replace_lemmas(df, lemma2update)
    df = eliminate_no_meaning_tokens(df, eliminated_words)
    
    make_paper_clean_file(df)
    return df


if __name__ == "__main__":
    df = pd.read_excel("TEST(8_2).xlsx")
    df.fillna("", inplace=True)
    df = postprocess_paper_df(df)
    df.to_excel("PAPER[OUTPUT].xlsx", index=False)