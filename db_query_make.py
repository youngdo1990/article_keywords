import pandas as pd
from tqdm import tqdm
from db_connect import *

def insert_queries(table_name):
    BATCH_SIZE = 500

    df = pd.read_excel("patent_domestic_clean.xlsx")
    

    columns = list(df.columns)
    queries = []
    insert_base = "INSERT INTO " + table_name + " ("
    for col in columns:
        insert_base += col + ", "
    insert_base = insert_base[:-2]
    insert_base += ") VALUES "

    count = 0

    # df = df.iloc[:100]

    insert = insert_base

    df = df.fillna("")
    for i in tqdm(df.index):
        for n, col in enumerate(columns):
            if n == 0:
                insert += "("
            insert += "'" + str(df[col][i]) + "', "
            if n == len(columns)-1:
                insert = insert[:-2]
                insert += "), "
        count += 1
        if count >= BATCH_SIZE:
            insert = insert[:-2]
            count = 0
            with open("log0713.sql", "+a", encoding="utf-8", errors="ignore") as file:
                file.write(insert + ";\n")
            queries.append(insert)
            conn = db_connection()
            cur = conn.cursor()
            insert = insert_base
            # cur.execute(queries[-1])
            # conn.commit()
        #     insert = insert_base
        #     count = 0
    conn.commit()


def recreate_table(table_name, table_columns="", drop_table=True):
    conn = db_connection()
    cur = conn.cursor()

    if table_columns == "":
        # query = "DESC " + table_name
        query = "DESC `" + table_name + "`"

        cur.execute(query)

        table_columns = cur.fetchall()


    if drop_table:
        print("DROP TABLE")
        query = "DROP TABLE IF EXISTS " + table_name
        cur.execute(query)
        conn.commit()
    
    print("CREATE TABLE")
    query = "CREATE TABLE IF NOT EXISTS `" + table_name + "` ("
    for col in table_columns:
        query += col[0] + " " + col[1]
        if col[2] == "NO":
            query += " NOT NULL"
        if col[3]:
            query += " " + col[3] 
        if col[4]:
            query += " DEFAULT" + col[4]
        if col[5]:
            query += " " + col[5]
        query += ", "
    query = query[:-2]
    query += ")"
    print(query)
    
    # cur.execute(query)
    # conn.commit()
    conn.close()


if __name__ == "__main__":
    # recreate_table(table_name = "FCT_PATENT_OVERSEAS_KEYWORDS", drop_table=False)
    insert_queries(table_name = "FCT_PATENT_DOMESTIC_KEYWORDS")