import configparser
import time
import pandas as pd

from urllib.parse import quote_plus

from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DATETIME, Index, text
from utils import log_manager


## config data 가져오기
config = configparser.ConfigParser()
config.read('../config/config.ini', encoding="UTF-8")
db_logger = log_manager.get_logger('db_manager')


class DBManager:

    def __init__(self):
        # DB setting info
        self._mariadb_host = config['DATABASE']['host']
        self._mariadb_port = int(config['DATABASE']['port'])
        self._mariadb_user = config['DATABASE']['user']
        self._mariadb_password = quote_plus(config['DATABASE']['password'])
        self._mariadb_db_schema = config['DATABASE']['db_schema']
        self._mariadb_charset = config['DATABASE']['charset']
        self._mariadb_connect_timeout = int(config['DATABASE']['connect_timeout'])

        # engine
        connect_args = {
            'connect_timeout': self._mariadb_connect_timeout
        }
        try:
            db_engine = create_engine("mysql+pymysql://"
                                      + f"{self._mariadb_user}:" + f"{self._mariadb_password}"
                                      + f"@{self._mariadb_host}:{self._mariadb_port}"
                                      + f"/{self._mariadb_db_schema}"
                                      + f"?charset={self._mariadb_charset}"
                                      , connect_args=connect_args)

            db_logger.info("create engine instance : connect success")
            db_logger.info(f"mariadb_host : {self._mariadb_host}")
            db_logger.info(f"mariadb_port : {self._mariadb_port}")
            db_logger.info(f"mariadb_user : {self._mariadb_user}")
            db_logger.info(f"mariadb_db_schema : {self._mariadb_db_schema}")
        except ConnectionError as e:
            db_logger.error(f"fail to create engine instance : {e}")
            self.db_engine = None
        else:
            self.db_engine = db_engine

    def insert(self, table_name, df):
        df = df.fillna('')
        self.create_table(table_name)

        # time check start
        start_time = time.perf_counter()

        db_logger.info(f'collecting table name: {table_name}')
        df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
        db_logger.info(f'inserted data count => {str(len(df))}')

        # time check end~
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        db_logger.info(f'{table_name} taken time(insert): {str(elapsed_time)}s')

    def select_data_from_paper(self) -> DataFrame:
        f"""
        This function is select data for extract keyword from FCT_PAPER_ABSTRACT.
        :return DataFrame
        """
        paper_table_name = config['DATABASE_TABLE']['PAPER_ABSTRACT']
        paper_keyword_table_name = config['DATABASE_TABLE']['PAPER_ABSTRACT_KEYWORDS']

        if self.db_engine.dialect.has_table(self.db_engine.connect(), paper_table_name):
            recent_update_date = self.select_create_date(paper_keyword_table_name)
            where_clause = f"WHERE CREATE_DTM > DATE_FORMAT('{recent_update_date}', '%Y-%m-%d %H:%M:%S')" if recent_update_date != '' else ''

            with self.db_engine.connect() as conn:
                query = text(f'''
                    SELECT  PMID
                            , CN
                            , TITLE
                            , ABSTRACT
                            , KEYWORD
                            , CREATE_DTM
                    FROM {paper_table_name}
                    {where_clause}
                ''')
                exe = conn.execute(query)
                result = exe.fetchall()

                if result:
                    df = pd.DataFrame(result)
                    return df
                else:
                    db_logger.warning(f'fail to select data: {paper_table_name} does not has data.')
                    return pd.DataFrame()
        else:
            return pd.DataFrame()

    def select_data_from_domestic(self) -> DataFrame:
        f"""
        This function is select data for extract keyword from FCT_PATENT_DOMESTIC.
        :return DataFrame
        """
        patent_domestic_table_name = config['DATABASE_TABLE']['PATENT_DOMESTIC']
        patent_domestic_keyword_table_name = config['DATABASE_TABLE']['PATENT_DOMESTIC_KEYWORDS']

        if self.db_engine.dialect.has_table(self.db_engine.connect(), patent_domestic_table_name):
            recent_update_date = self.select_create_date(patent_domestic_keyword_table_name)
            where_clause = f"WHERE CREATE_DTM > DATE_FORMAT('{recent_update_date}', '%Y-%m-%d %H:%M:%S')" if recent_update_date != '' else ''

            with self.db_engine.connect() as conn:
                query = text(f'''
                    SELECT  MST_ID
                            , INVENTION_TITLE_KO
                            , INVENTION_TITLE_EN
                            , ABSTRACT_CONTENTS
                            , CREATE_DTM
                    FROM {patent_domestic_table_name}
                    {where_clause}
                ''')
                exe = conn.execute(query)
                result = exe.fetchall()

                if result:
                    df = pd.DataFrame(result)
                    return df
                else:
                    db_logger.warning(f'fail to select data: {patent_domestic_table_name} does not has data.')
                    return pd.DataFrame()
        else:
            return pd.DataFrame()

    def select_data_from_overseas(self) -> DataFrame:
        f"""
        This function is select data for extract keyword from FCT_PATENT_OVERSEAS.
        :return DataFrame
        """
        patent_overseas_table_name = config['DATABASE_TABLE']['PATENT_OVERSEAS']
        patent_overseas_keyword_table_name = config['DATABASE_TABLE']['PATENT_OVERSEAS_KEYWORDS']

        if self.db_engine.dialect.has_table(self.db_engine.connect(), patent_overseas_table_name):
            recent_update_date = self.select_create_date(patent_overseas_keyword_table_name)
            where_clause = f"WHERE CREATE_DTM > DATE_FORMAT('{recent_update_date}', '%Y-%m-%d %H:%M:%S')" if recent_update_date != '' else ''

            with self.db_engine.connect() as conn:
                query = text(f'''
                        SELECT  MST_ID
                                , INVENTION_TITLE_KO
                                , INVENTION_TITLE_EN
                                , ABSTRACT_CONTENTS
                                , CREATE_DTM
                        FROM {patent_overseas_table_name}
                        {where_clause}
                    ''')
                exe = conn.execute(query)
                result = exe.fetchall()

                if result:
                    df = pd.DataFrame(result)
                    return df
                else:
                    db_logger.warning(f'fail to select data: {patent_overseas_table_name} does not has data.')
                    return pd.DataFrame()
        else:
            return pd.DataFrame()

    def select_data_from_project(self) -> DataFrame:
        f"""
        This function is select data for extract keyword from FCT_PROJECT_ABSTRACT.
        :return DataFrame
        """
        project_table_name = config['DATABASE_TABLE']['PROJECT_ABSTRACT']
        project_keyword_table_name = config['DATABASE_TABLE']['PROJECT_ABSTRACT_KEYWORDS']

        if self.db_engine.dialect.has_table(self.db_engine.connect(), project_table_name):
            recent_update_date = self.select_create_date(project_keyword_table_name)
            where_clause = f"WHERE CREATE_DTM > DATE_FORMAT('{recent_update_date}', '%Y-%m-%d %H:%M:%S')" if recent_update_date != '' else ''

            with self.db_engine.connect() as conn:
                query = text(f'''
                        SELECT  RES_NO
                                , PROJ_NO
                                , TITLE_KR
                                , TITLE_EN
                                , ABSTRACT_TEASER
                                , EFFECT_TEASER
                                , KEYWORD_KR
                                , KEYWORD_EN
                                , CREATE_DTM
                        FROM {project_table_name}
                        {where_clause}
                    ''')
                exe = conn.execute(query)
                result = exe.fetchall()

                if result:
                    df = pd.DataFrame(result)
                    return df
                else:
                    db_logger.warning(f'fail to select data: {project_table_name} does not has data.')
                    return pd.DataFrame()
        else:
            db_logger.warning(f'fail to select data: {project_table_name} does not exist.')
            return pd.DataFrame()

    def select_create_date(self, table_name) -> str:
        f"""
        This function is select most recently CREATE_DTM data from table_name.
        :param table_name: Table name(str)
        :return str
        """
        if self.db_engine.dialect.has_table(self.db_engine.connect(), table_name):
            with self.db_engine.connect() as conn:
                query = text(f'''
                    SELECT CREATE_DTM
                    FROM {table_name}
                    ORDER BY CREATE_DTM DESC
                    LIMIT 1
                ''')
                exe = conn.execute(query)
                result = exe.fetchone()

                if result:
                    date_str = result[0].strftime('%Y-%m-%d %H:%M:%S')
                    return date_str
                else:
                    db_logger.warning(f'fail to select CREATE_DTM: {table_name} does not has CREATE_DTM.')
                    return ''
        else:
            db_logger.warning(f'fail to select CREATE_DTM: {table_name} does not exist.')
            return ''

    def create_table(self, table_name):
        f"""
        This function is CREATE TABLE by {table_name}.
        :param table_name: Table name(str)
        """
        if not self.db_engine.dialect.has_table(self.db_engine.connect(), table_name):
            metadata = MetaData()
            if table_name == 'FCT_PAPER_KEYWORDS':
                Table(table_name, metadata
                      , Column('PMID', String(10))
                      , Column('CN', String(30))
                      , Column('KEYWORD', String(50))
                      , Column('FREQ', Integer)
                      , Column('CREATE_DTM', DATETIME, nullable=False)
                      , Index('WORDCLOUD_INDX', 'PMID', 'CN')
                      , mysql_collate='utf8mb3_general_ci')
            elif table_name == 'FCT_PATENT_DOMESTIC_KEYWORDS':
                Table(table_name, metadata
                      , Column('MST_ID', String(18))
                      , Column('KEYWORD', String(30))
                      , Column('FREQ', Integer)
                      , Column('CREATE_DTM', DATETIME, nullable=False)
                      , Index('WORDCLOUD_INDX', 'MST_ID')
                      , mysql_collate='utf8mb3_general_ci')
            elif table_name == 'FCT_PATENT_OVERSEAS_KEYWORDS':
                Table(table_name, metadata
                      , Column('MST_ID', String(18))
                      , Column('KEYWORD_EN', String(50))
                      , Column('KEYWORD_KR', String(30))
                      , Column('FREQ', Integer)
                      , Column('CREATE_DTM', DATETIME, nullable=False)
                      , Index('WORDCLOUD_INDX', 'MST_ID')
                      , mysql_collate='utf8mb3_general_ci')
            elif table_name == 'FCT_PROJECT_KEYWORDS':
                Table(table_name, metadata
                      , Column('RES_NO', String(18))
                      , Column('KEYWORD_EN', String(50))
                      , Column('KEYWORD_KR', String(30))
                      , Column('FREQ', Integer)
                      , Column('CREATE_DTM', DATETIME, nullable=False)
                      , Index('WORDCLOUD_INDX', 'RES_NO')
                      , mysql_collate='utf8mb3_general_ci')

            db_logger.info('created table : ' + table_name)
            metadata.create_all(self.db_engine)


def main():
    # 1. DBManager 객체 생성
    db_manager = DBManager()

    # 2. 테이블 데이터 조회 예제
    #    (이전 데이터가 있을 경우, 증분 데이터만 반환)
    select_df = db_manager.select_data_from_domestic()
    print(select_df)

    # 3. 데이터 삽입 예제
    #    (데이터를 넣을 테이블 명, 데이터를 파라미터로 전달)
    #    !! CREATE_DTM의 값으로 select_df로 받은 CREATE_DTM을 사용 !!
    insert_df = pd.DataFrame()
    db_manager.insert('FCT_PATENT_DOMESTIC_KEYWORDS', insert_df)


if __name__ == "__main__":
    main()
