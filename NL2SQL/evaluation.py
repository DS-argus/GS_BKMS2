import json
import pandas as pd

from openai import OpenAI
from DB import MyPostgreSQL

class NO2SQL():

    def __init__(self, data: pd.DataFrame, ddl: pd.DataFrame) -> None :
        self.spider = data
        self.ddl = ddl
        
        self.db = MyPostgreSQL()
        self.db.login()

        self.llm = None

    def connect_GPT_API(self) -> None:
        
        with open("info.json", 'r') as file:
            info = json.load(file)
            OPENAI_API_KEY = info['gpt_info']['API_key']
        
        if OPENAI_API_KEY is None:
            raise ValueError("API key not found in .env file")
        
        self.llm = OpenAI(api_key = OPENAI_API_KEY)

    def qNL_to_qSQL(self, qNL: str, schema: str = None, prompt: str = None, token: bool = False) -> str:
        self.connect_GPT_API()

        # 'system': gpt-4에게 어떤 task를 수행할 지 지시합니다.
        # 'user': qNL, schema 등 입력할 내용을 넣어줍니다.
        system_message = "Translate the following natural language question into a SQL query compatible with PostgreSQL."
        if schema:
            schema = self.spider[self.spider['question'] == qNL]['CREATE']
            system_message += f" The schema of database is as following: {schema}"

        ## prompt 어떤식으로 반영할지 고민해야 함
        if prompt:
            pass

        response = self.llm.chat.completions.create(
                        model= "gpt-3.5-turbo",
                        messages= [
                                    {"role": "system", "content": system_message},
                                    {"role": "user", "content": qNL},
                                  ],
                        temperature= 0
                    )
        
        # 반환 받은 response에서 쿼리만 따로 추출합니다.
        qSQL = response.choices[0].message.content.replace('\n', '')        
        
        # 예) qNL = 'How many departments are led by heads who are not mentioned?'
        #    qSQL = 'SELECT COUNT(*) FROM departments WHERE head_id NOT IN (SELECT id FROM heads)'
        
        if token:
            print(response.usage)

        return qSQL

    def create_schema(self, db_id: str):
        # db_id에 해당하는 table 생성 및 데이터 삽입
        # 테이블 이름이 동일한 경우가 많아서 단순하게 매번 새롭게 table 삭제 -> table 생성 -> 데이터 삽입 과정 거침

        # 기존 테이블 모두 제거
        self.db.drop_all_tables()

        # db_id에 해당하는 table 추출
        current_schema = self.ddl[self.ddl['db_id'] == db_id].copy()
        if len(current_schema) == 0:
            print(1)
            print("db_id does not exist")
        
        try:
            for row in current_schema.iterrows():
                create_qry, insert_qry = row[1]['CREATE'], row[1]['INSERT']
                self.db.execute_query(create_qry)
                self.db.execute_query(insert_qry)
            
            print("Created schema successfully")
        
        except Exception as e:
            print(f"Error: {str(e)}")


    def get_SQL_answer(self, qNL: str) -> str:

        return self.spider[self.spider['question'] == qNL].loc[:, 'query'].values[0]

    def working_test(self, db_id, qNL):
        
        # spider 데이터셋에 있는 정답 쿼리       
        true_SQL = self.get_SQL_answer(qNL)
        print(f"true_SQL : {true_SQL}")
        
        self.create_schema(db_id)
        
        true_SQL_result = self.db.select_query(true_SQL)
        print(f"true_SQL_result : \n{true_SQL_result}")
    

    # 한 question에 대한 NO2SQL 결과
    def execute_NL2SQL(self, db_id: str, qNL: str, schema: str = None, prompt: str = None, token: bool = False) -> tuple:

        # spider 데이터셋에 있는 정답 쿼리       
        true_SQL = self.get_SQL_answer(qNL)
        print(f"true_SQL : {true_SQL}")
        # gpt로부터 얻은 쿼리
        qSQL = self.qNL_to_qSQL(qNL, schema, prompt, token)
        print(f"qSQL : {qSQL}")
        # 필요한 테이블 및 데이터 추가
        self.create_schema(db_id)

        # 정답쿼리로 출력한 결과
        true_SQL_result = self.db.select_query(true_SQL)
        print(f"true_SQL_result : {true_SQL_result}")
        # qSQL로 출력한 쿼리
        qSQL_result = self.db.select_query(qSQL)
        print(f"qSQL_result : {qSQL_result}")

        result = (true_SQL, qSQL, true_SQL_result, qSQL_result)

        return result

    def run_NO2SQLs(self, schema: bool = False, prompt: bool = False, token: bool = False) -> pd.DataFrame:

        # 전체 데이터에 대해서 execute_NL2SQL 반복해서 결과를 dataframe으로 저장

        total_result = []
        for row in self.spider.iterrows():
            db_id, qNL = row[1]['db_id'], row[1]['question']
            print(f"qNL : {qNL}")
            
            result = self.execute_NL2SQL(db_id, qNL, schema, prompt, token)
            total_result.append(result)

        total_result = pd.DataFrame(total_result, columns=['true_SQL', 'qSQL', 'true_SQL_result', 'qSQL_result'])
        
        return total_result

    def run_NL2SQLs_test(self):
        for row in self.spider.iterrows():
            db_id, qNL = row[1]['db_id'], row[1]['question']
            print(db_id, qNL)
            self.working_test(db_id, qNL)


if __name__ == "__main__":

    # 10개만 실행
    # i = 10
    # spider = pd.read_csv("rawdata/SPIDER_SELECTED.csv", index_col=0).iloc[0:10, :]
    
    # 전체 실행
    spider = pd.read_csv("rawdata/SPIDER_SELECTED.csv", index_col=0)
    ddl = pd.read_csv("rawdata/DDL_SELECTED.csv")

    task = NO2SQL(spider, ddl)
    result = task.run_NL2SQLs_test()

    print(result)


        

