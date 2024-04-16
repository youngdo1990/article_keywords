# -*- coding: utf-8 -*-
from paper import *
from project import *
from patent import *

PATENT_DOMESTIC_KEYWORDS = os.path.join("특허2", "domestic", "patent_keywords_kor.xlsx")
PATENT_OVERSEAS_KEYWORDS = os.path.join("특허2", "overseas", "patent_keywords_total.xlsx")
PROJECT_KEYWORDS = os.path.join("연구과제2", "TOTAL", "project_keywords_total.xlsx")
PAPER_KEYWORDS = os.path.join("논문2", "paper_keywords.xlsx")


def get_dataframes():
    paper_df = pd.read_excel(PAPER_KEYWORDS)
    project_df = pd.read_excel(PROJECT_KEYWORDS)
    overseas_df = pd.read_excel(PATENT_OVERSEAS_KEYWORDS)
    domestic_df = pd.read_excel(PATENT_DOMESTIC_KEYWORDS)
    return paper_df, project_df, overseas_df, domestic_df


def preprocessing(truncate=False):
    if not truncate:
        paper_df, project_df, overseas_df, domestic_df = get_dataframes()
    else:
        paper_df = project_df = overseas_df = domestic_df = None
    paper_keywords(paper_df)
    investigation_task_keywords(project_df)
    domestic_patent_keywords(overseas_df)
    overseas_patent_keywords(domestic_df)
    return 0


if __name__ == "__main__":
    preprocessing(truncate=False)