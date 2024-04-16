# -*- coding: utf-8 -*-

from text_cleaner import *


def eng_freq_counter(word, text):
    if text != None:
        text = text.replace("-", " ")
    else:
        return 0
    text_words = text.split()
    count = 0
    text_words = clean_eng_keywords(text_words)
    for w in text_words:
        if w == word:
            count += 1
    return count


def kor_freq_counter(word, text):
    text_words = text.split()
    count = 0
    text_words = clean_ko_keywords(text_words)

    for w in text_words:
        if w == word:
            count += 1
    return count