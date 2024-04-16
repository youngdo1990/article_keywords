# -*- coding: utf-8 -*-

from tools import *
from text_cleaner import * 
from frequency_counter import *
import os


## RAKE
from rake_nltk import Rake
## OTHER KEYWORD EXTRACTION
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')

OUTPUT_FOLDER = "논문2"
TABLE = "FCT_PAPER_ABSTRACT"
EXPECTED_KEYWORDS_NUMBER = 20
MIN_FREQ = 2
ENGLISH_STOPWORDS = set(stopwords.words('english'))


def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


def complement_with_rake(title, abstract, keywords, rake):
    text = title + "\n" + abstract
    rake.extract_keywords_from_text(text)
    rake_keywords = dict(sorted(rake.frequency_dist.items(), key=lambda item:item[1], reverse=True))
    # rake_keywords = {key for key in rake_keywords if key not in ENGLISH_STOPWORDS}
    rake_keywords = {key:rake_keywords[key] for key in rake_keywords if rake_keywords[key] > MIN_FREQ and key.isalpha() and key!="et" and key!="al" and key!="ad" and len(key)>1}
    for key in rake_keywords:
        if key not in keywords:
            keywords[key] = rake_keywords[key]
    keywords = dict(sorted(keywords.items(), key=lambda item:item[1], reverse=True))
    return keywords


def paper_keywords(checkpoint=True, result=None):
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
    pmid = []
    cn = []
    keyword = []
    freq = []
    if result.empty:
        source_file = "FCT_PAPER_ABSTRACT"
        df = pd.read_excel(source_file)
        result = get_dataframes_tuple(df)
        
        if checkpoint and os.path.exists(os.path.join(OUTPUT_FOLDER, "paper_keywords.xlsx")):
            # CHEKPOINT LOAD 
            check = pd.read_excel(os.path.join(OUTPUT_FOLDER, "paper_keywords.xlsx"))
            start_index = len(check.index)

            pmid = check["PMID"].to_list()
            cn = check["CN"].to_list()
            keyword = check["KEYWORD"].to_list()
            freq = check["FREQ"].to_list()

            result = result[start_index:]
        else:
            result = get_dataframes_tuple(result)
    else:
        result = get_dataframes_tuple(result)
            
    count = 0
    rake = Rake()
    print()
    for r in tqdm(result):
        title = r[2]
        abstract = r[3]
        title_keywords = eng_model.extract_keywords(title, top_n=50,  nr_candidates=100)
        abstract_keywords = eng_model.extract_keywords(abstract, top_n=50,  nr_candidates=100)
        
        keywords = [w[0] for w in title_keywords]
        for w in abstract_keywords:
            if w not in abstract_keywords:
                keywords.append(w[0])
        
        keywords_freq = [eng_freq_counter(f, title + "\n" + abstract) for f in keywords]
        keywords_dict = {i:j for i, j in zip(keywords, keywords_freq)}
        keywords_dict = {key:keywords_dict[key] for key in keywords_dict if keywords_dict[key] > 0}
        keywords_dict = dict(sorted(keywords_dict.items(), key=lambda item:item[1], reverse=True))

        if len(keywords_dict) < EXPECTED_KEYWORDS_NUMBER:
            keywords_dict = complement_with_rake(title, abstract, keywords_dict, rake)
            # print(keywords_dict)
            # print(len(keywords_dict))
        for k in keywords_dict:
            pmid.append(r[0])
            cn.append(r[1])
            keyword.append(k)
            freq.append(keywords_dict[k])
        # 사전별 데이터 프레임 만들기
        keywords_df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])
        keywords_df["PMID"] = pmid
        keywords_df["CN"] = cn
        keywords_df["KEYWORD"] = keyword
        keywords_df["FREQ"] = freq
        count += 1
        if count >= 100:
            count = 0
            keywords_df.to_excel(os.path.join(OUTPUT_FOLDER, "paper_keywords.xlsx"), index=False)
       
    
    keywords_df = pd.DataFrame(columns=["PMID", "CN", "KEYWORD", "FREQ"])
    keywords_df["PMID"] = pmid
    keywords_df["CN"] = cn
    keywords_df["KEYWORD"] = keyword
    keywords_df["FREQ"] = freq
    # keywords_df.to_excel(os.path.join(OUTPUT_FOLDER, "paper_keywords.xlsx"), index=False)
    return keywords_df


if __name__ == "__main__":
    paper_keywords(checkpoint=True)