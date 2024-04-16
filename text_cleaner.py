# -*- coding: utf-8 -**
import nltk
from konlpy.tag import Kkma

# 불용어 다운로드
nltk.download("stopwords")


JOSA = ["JKG", "JKO", "ETD", "XSN", "JKM", "ETD", "NR", "VCP"] # Kkma 조사


def clean_eng_keywords(keywords):
    stopwords = nltk.corpus.stopwords.words("english")
    keywords = [key for key in keywords if key.lower() not in stopwords]
    keywords = [key for key in keywords if len(key)>1]
    return keywords


def clean_ko_keywords(keywords):
    kkma = Kkma()
    cleaned_keywords = []
    for key in keywords:
        pos = kkma.pos(key)
        # print(pos)
        reverse_pos_list = []
        for i in reversed(range(len(pos))):
            if pos[i][1] in JOSA:
                reverse_pos_list.append(pos[i][0])
            else:
                break
        pos_list = [p for p in reversed(reverse_pos_list)]
        josa = "".join(pos_list)
        word = key.replace(josa, "")
        cleaned_keywords.append(word)
        # print(word)
    cleaned_keywords = [w for w in cleaned_keywords if len(w) > 1]
    return cleaned_keywords