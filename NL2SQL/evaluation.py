import pandas as pd
import json
from DB import MyPostgreSQL

class NO2SQL():

    def __init__(self) -> None :
        self.data = None
        self.db = None

    def load_data(self, path: str) -> None :
        # 데이터는 merged.csv + 각 query 마다 prompt 정보 열도 추가해야하나... prompt를 미리 다 세팅할 수 있나
        self.data = pd.read_csv(path)

    def connect_DB(self) -> None :
        self.db = MyPostgreSQL()
        self.db.login()
    
    # 초기 설정
    def create_all_tables(self) -> None : 

        self.connect_DB()
        
        CREATE_queries = self.data['CREATE'].unique()

        for query in CREATE_queries:
            self.db.execute_query(query)
        
    # 초기 설정
    def insert_all_tables(self) -> None : 

        self.connect_DB()

        INSERT_queries = self.data['INSERT'].unique()

        for query in INSERT_queries:
            self.db.execute_query(query)
    

    def get_answer_qSQL(self, qNL: str) -> str:
        qSQL = self.data[self.data['question'] == qNL]['query']

        return qSQL


    # 안써봐서 모름
    def connect_GPT_API(self) -> None:
        with open("info.json", 'r') as file:
            info = json.load(file)
            API_key = info['gpt_info']['API_key']

        pass

    def send_qNL_to_GPT(self, qNL: str, schema: str = None, prompt: str = None) -> None:
        self.connect_GPT_API()
        pass

    def receive_qSQL_from_GPT(self) -> str:
        self.connect_GPT_API()
        pass


    # 한 question에 대한 NO2SQL 결과
    def execute_NL2SQL(self, qNL: str, schema: str = None, prompt: str  =None) -> tuple:
        
        true_qSQL = self.get_answer_qSQL(qNL)
        true_qSQL_result = self.db.execute(true_qSQL)
        
        self.send_qNL_to_GPT(qNL)

        qSQL = self.receive_qSQL_from_GPT()
        qSQL_result = self.db.execute(qSQL)

        result = (true_qSQL, true_qSQL_result, qSQL, qSQL_result)

        return result


    def run_NO2SQLs(self) -> pd.DataFrame:

        ## 전체 데이터에 대해서 execute_NL2SQL 반복해서 결과를 dataframe으로 저장
        pass


if __name__ == "__main__":

    task = NO2SQL()
    task.connect_DB()
    qry = "SELECT datname FROM pg_database;"
    print(task.db.excute_query(qry))

