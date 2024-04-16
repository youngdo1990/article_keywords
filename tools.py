# -*- coding: utf-8 -*-

from tqdm import tqdm
import pandas as pd
from keybert import KeyBERT

from tqdm import tqdm
from db_connect import *


# 모델 적재
# kw_model.extract_keywords(doc, keyphrase_ngram_range=(2, 2), use_maxsum=True,  top_n=10,  nr_candidates=20)
kr_model = KeyBERT(model="paraphrase-multilingual-MiniLM-L12-v2") # 한국어 지원할 수 있는 모델
eng_model = KeyBERT(model="biobert-v1.1") # BioBERT 영어 성능 제일 좋아서 영어 적혀 있는 경우에 BioBERT 가중치 적제
# 모델 적제
