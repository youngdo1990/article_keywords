import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.stem import WordNetLemmatizer
import pandas as pd
from tqdm import tqdm
from db_connect import *
import os

import itertools


def get_corpus_from_paper(table_name):
    ## Lemmatizer
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    
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
        if r[2] not in corpus_words:
            corpus_words.append(r[2])
    conn.close()

    corpus_words = [nltk.pos_tag([w])[0] for w in corpus_words]
    return corpus_words


def lemma_correction(lemma2update):
    conn = db_connection()
    cur = conn.cursor()
    table_name = "FCT_PAPER_KEYWORDS"

    # lemma2update = dict(itertools.islice(lemma2update.items(), 2))


    for l in tqdm(lemma2update):
        query = "SELECT * FROM " + table_name + " WHERE KEYWORD='" + l + "'"
        cur.execute(query)
        result = cur.fetchall()

        for r in tqdm(result):
            query2 = "SELECT KEYWORD, FREQ FROM " + table_name + " WHERE PMID='" + r[0] + "' AND CN='" + r[1] + "'"
            cur.execute(query2)
            result2 = cur.fetchall()
            paper_keywords = [w[0] for w in result2]
            keywords_freq = [w[1] for w in result2]
            if lemma2update[l] in paper_keywords and l in paper_keywords:
                modified_keywords = {}
                for k, f in zip(paper_keywords, keywords_freq):
                    if k != l:
                        modified_keywords[k] = f
                    else:
                        if k not in modified_keywords:
                            modified_keywords[lemma2update[k]] = f
                        else:
                            modified_keywords[lemma2update[k]] += f
                modified_keywords = dict(sorted(modified_keywords.items(), key=lambda item:item[1], reverse=True))
                # 중복 예방
                delete_query = "DELETE FROM " + table_name + " WHERE PMID='" + r[0] + "' AND CN='" + r[1] + "'"
                cur.execute(delete_query)
                conn.commit()
                # 키워드 업데이트
                print(f"PMID{r[0]} - CN={r[1]}")
                for w in modified_keywords:
                    update_query = "INSERT INTO " + table_name + " (PMID, CN, KEYWORD, FREQ) VALUES("
                    update_query += "'" + str(r[0]) + "', '" + r[1] + "', '" + w + "', '" + str(modified_keywords[w]) + "')"
                    cur.execute(update_query)
                    conn.commit()
                    # print(update_query)
            else:
                # print(f"PMID{r[0]} - CN={r[1]}")
                update_query = "UPDATE " + table_name + " SET KEYWORD='" + lemma2update[l] + "' WHERE PMID='" + r[0] + "' AND CN='" + r[1] + "' AND KEYWORD='" + l + "'"
                cur.execute(update_query)
                conn.commit()
                pass
    conn.close()
    return


def eliminate_no_meaning_tokens(table_name, eliminated_words):
    # 의미 없는 키워드 제거
    conn = db_connection()
    cur = conn.cursor()

    print("숫자, 형용사, 부사 삭제 중입니다")
    for w in tqdm(eliminated_words):
        query = "DELETE FROM " + table_name + " WHERE KEYWORD='" + w + "'"
        cur.execute(query)
        result = cur.fetchall()
        conn.commit()

    conn.close()
    return


def eliminate_english_words(table_name):
    ## Lemmatizer
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    corpus_words = get_corpus_from_paper(table_name)
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
    
    lemma_correction(lemma2update)
    eliminate_no_meaning_tokens(table_name, eliminated_words)
    return


if __name__ == "__main__":
    # eliminate_english_words("FCT_PAPER_KEYWORDS")
    table_name = "FCT_PAPER_KEYWORDS"

    ## Lemmatizer
    nltk.download("wordnet")
    nltk.download('averaged_perceptron_tagger')
    lemmatizer = WordNetLemmatizer()
    corpus_words = get_corpus_from_paper(table_name)
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
    print(lemma2update)
    # eliminate_no_meaning_tokens(table_name, eliminated_words)
    # lemma_correction(lemma2update)
