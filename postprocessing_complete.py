# -*- coding: utf-8 -*-

import os
import pandas as pd
from paperdf_postprocessing import *
from projectdf_postprocesing import *
from domesticdf import *
from overseasdf import *


def get_dataframes():
    PATENT_DOMESTIC_KEYWORDS = os.path.join("특허2", "domestic", "patent_keywords_kor.xlsx")
    PATENT_OVERSEAS_KEYWORDS = os.path.join("특허2", "overseas", "patent_keywords_total.xlsx")
    PROJECT_KEYWORDS = os.path.join("연구과제2", "TOTAL", "project_keywords_total.xlsx")
    PAPER_KEYWORDS = os.path.join("논문2", "paper_keywords.xlsx")
    paper_df = pd.read_excel(PAPER_KEYWORDS)
    project_df = pd.read_excel(PROJECT_KEYWORDS)
    overseas_df = pd.read_excel(PATENT_OVERSEAS_KEYWORDS)
    domestic_df = pd.read_excel(PATENT_DOMESTIC_KEYWORDS)
    return paper_df, project_df, overseas_df, domestic_df


def get_dataframes_tuple(df):
    return list(df.itertuples(index=False, name=None))


def postprocessing(paper_df, project_df, overseas_df, domestic_df):
    paper_df.fillna("", inplace=True)
    project_df.fillna("", inplace=True)
    overseas_df.fillna("", inplace=True)
    domestic_df.fillna("", inplace=True)

    postprocess_paper_df(paper_df)
    project_df = get_dataframes_tuple(project_df)
    postprocess_project(project_df)
    domestic_df = get_dataframes_tuple(domestic_df)
    postprocess_domestic(domestic_df)
    overseas_df = get_dataframes_tuple(overseas_df)
    overseas_postprocessing(overseas_df)
    return 0


if __name__ == "__main__":
    paper_df, project_df, overseas_df, domestic_df = get_dataframes()
    postprocessing()