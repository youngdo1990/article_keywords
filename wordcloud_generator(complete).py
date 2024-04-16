# -*- coding: utf-8 -*-
import pandas as pd
import os
from tqdm import *
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
from db_connect import *

WORDCLOUD_OUTPUT = "워드클라우드"


def complete_text_wordcloud():
    conn = db_connection()
    cur = conn.cursor()

    query = "SELECT * FROM FCT_PAPER_ABSTRACT"

    df = pd.read_sql(query, conn)

    df.head()

    if not os.path.exists(WORDCLOUD_OUTPUT):
        os.makedirs(WORDCLOUD_OUTPUT)

    for i in tqdm(df.index):
        if df["PMID"][i]:
            paper_id = df["PMID"][i]
        else:
            paper_id = df["CN"][i]
        text = df["TITLE"][i]
        if df["ABSTRACT"][i] != None:
            text += "\n" + df["ABSTRACT"][i]
        
        wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate(text)
        plt.figure()
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")
        # plt.show()
        plt.savefig(os.path.join(WORDCLOUD_OUTPUT, paper_id + ".png"))


if __name__ == "__main__":
    complete_text_wordcloud()