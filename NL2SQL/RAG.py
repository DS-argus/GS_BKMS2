import re
import json
import pandas as pd

from openai import OpenAI
from DB import MyPostgreSQL
from datetime import datetime


from langchain.embeddings.openai import OpenAIEmbeddings


class RAG_NO2SQL():

    def __init__(self) -> None :
        """
        self.spider : qNL, qSQL, schema, prompt가 있는 데이터셋
        self.db : DB.py를 통해 local postresql로 연결
        self.llm : chatgpt API
        """
        self.db = MyPostgreSQL()
        self.db.login()

        self.rag_sample = pd.read_csv("./presentation/rag_sample.csv")
        self.llm = None
        self.embeddings = None

    def connect_GPT_API(self) -> None:
        """
        API_key를 가져와서 OpenAI API를 self.llm에 할당
        """
        
        with open("info.json", 'r') as file:
            info = json.load(file)
            OPENAI_API_KEY = info['gpt_info']['API_key']
        
        if OPENAI_API_KEY is None:
            raise ValueError("API key not found in .env file")
        
        self.llm = OpenAI(api_key = OPENAI_API_KEY)

        self.embeddings = OpenAIEmbeddings(
                            model = "text-embedding-ada-002",
                            openai_api_key = OPENAI_API_KEY 
                        )

    def make_issue_table(self):

        create = """create table if not exists issue_tbl (id int,clause text,prompt text,primary key (id));"""
        insert = """insert into issue_tbl values (1, 'order by, limit', 'Rather than using the LIMIT clause, you can use the WHERE clause to find all rows that match the highest (lowest) value.');
                    insert into issue_tbl values (2, 'group by', 'Consider scenarios where objects share the same name. After group by clause, it would be better to use discinct id column');
                    insert into issue_tbl values (3, 'where, in', 'When utilizing where clause and in clause, it would be better to use INTERSECT or UNION clause');"""

        self.db.execute_query(create)
        self.db.execute_query(insert)

        return

    def make_VectorDB(self):
        self.rag_sample.columns = ['question', 'schema_info', 'embeddings']

        question = self.rag_sample['question']
        embeddings = self.rag_sample['embeddings']
        schema_info = self.rag_sample['schema_info']

        # 처음 pgVector를 사용하기 위해 필요
        self.db.execute_query("CREATE EXTENSION IF NOT EXISTS vector")

        table_create_command = '''
                                    DROP TABLE IF EXISTS promptbase;
                                    CREATE TABLE promptbase (
                                                id serial, 
                                                question varchar,
                                                embeddings vector(1536),
                                                schema_info varchar
                                                );
                             '''            
        self.db.execute_query(table_create_command)

        cursor = self.db.conn.cursor()
        for q, emb, schema in zip(question, embeddings, schema_info):
            query = '''
                        INSERT INTO promptbase (question, embeddings, schema_info) 
                        VALUES (%s, %s::vector, %s);
                    '''
            cursor.execute(query, (q, emb, schema))
        
        self.db.conn.commit()
        cursor.close()
        self.db.conn.close()

        print("Created promptbase database!")

    def embedding(self, qNL: str):
        """
        자연어가 들어오면 embedding해주는 함수
        """

        self.connect_GPT_API()

        embedded_vec = self.embeddings.embed_query(text=qNL)

        return embedded_vec
        
    def vector_search(self, qNL_emb: list, k: int = 5):
        """
        저장된 pgVector DB에서 임베딩 유사도를 구해서 상위 5개를 출력하고
        majority vote으로 적합한 db_schema를 알려주는 함수
        """

        search_qry = f""" 
                      SELECT schema_info, question, 1-(embeddings <=> '{qNL_emb}')FROM promptbase
                      ORDER BY embeddings <=> '{qNL_emb}'
                      LIMIT {k};
                    """
        result = self.db.select_query(search_qry)

        # Count the occurrences of each value in the 'schema_info' column
        schema_info_counts = result.iloc[:,0].value_counts()

        # Find the value with the maximum count in the 'schema_info' column
        most_common_schema_info = schema_info_counts.idxmax()

        return most_common_schema_info


    def get_prompt_by_issue(self, case: int):
        """
        SQL clause에 따른 추가 프롬프트를 담고 있는 DB에서 필요한 프롬프트 return
        """

        qry = f"SELECT prompt from issue_tbl where id = {case};"
        prompt = self.db.select_query(qry)
        
        return prompt.iloc[0,0]


    def qNL_to_qSQL(self, model: str, qNL: str, token: bool = False) -> str:
        """
        model : 사용할 model - gpt-3.5-turbo, gpt-4
        qNL : gpt한테 보낼 자연어 질문
        token : API 사용량 출력
        """
        self.connect_GPT_API()

        searched_schema = self.vector_search(self.embedding(qNL))

        cautions = (
                        "1. Care about the upper and lower case of column names\n"
                        "2. You should match string column regardless of upper or lower case\n"
                        "3. Only output the elements mentioned in the question\n"
                        "4. Avoid outputting duplicate value\n"
                    )
         
        # 'system': gpt-4에게 어떤 task를 수행할 지 지시합니다.
        # 'user': qNL, schema 등 입력할 내용을 넣어줍니다.
        system_message = (
                            "[Instruction]\n"                 
                            "Translate the following natural language question into a SQL query compatible with PostgreSQL.\n"
                            "You will get additional information, such as DB schema, meaning of each column, relatioship between tables, common mistakes and so on.\n"
                            "The schema is organized in the form of a JSON file, so check it out to see what information it contains before answering.\n"
                         )

        system_message += f"\n[The schema of database]\n{searched_schema}\n"

        system_message += f"\n[Common mistakes]\n{cautions}\n"

        system_message +=f"\n[Question]\n{qNL}\n"

        system_message += "\nPlease analyze the information carefully and give me an answer without explanation.\n"

        response = self.llm.chat.completions.create(
                        model= model,
                        messages= [
                                    {"role": "system", "content": system_message},
                                    {"role": "user", "content": ""},
                                  ],
                        temperature= 0
                    )
        
        # 반환 받은 response에서 쿼리만 따로 추출합니다.
        qSQL = response.choices[0].message.content.replace('\n', ' ')        
        
        # 만약 qSQL 안에 "Order by" and "Limit", "Group by", "Where" and "In"이 있으면 각각 1, 2, 3 Case로 issue_tbl에서 찾아서 prompt 전달
        # Check for the presence of keywords
        
        lower_qSQL =  qSQL.lower()

        has_order_by = re.search(r'\border\s+by\b', lower_qSQL)
        has_limit = re.search(r'\blimit\b', lower_qSQL)
        has_group_by = re.search(r'\bgroup\s+by\b', lower_qSQL)
        has_where = re.search(r'\bwhere\b', lower_qSQL)
        has_in = re.search(r'\bin\b', lower_qSQL)
        
        # Branch into 1, 2, or 3 cases based on the presence of keywords
        if (has_order_by and has_limit) or has_group_by or (has_where and has_in):
            system_message += "\n[Highlighting]\n"
    
            # Case 1 
            if (has_order_by and has_limit):
                system_message += f"{self.get_prompt_by_issue(1)}\n"
            # Case 2
            elif has_group_by:
                system_message += f"{self.get_prompt_by_issue(2)}\n"
            # Case 3
            elif (has_where and has_in):
                system_message += f"{self.get_prompt_by_issue(3)}\n"

            # 프롬프트를 추가해서 다시 LLM에 요청
            response = self.llm.chat.completions.create(
                    model= model,
                    messages= [
                                {"role": "system", "content": system_message},
                                {"role": "user", "content": ""},
                                ],
                    temperature= 0
                )

            # 반환 받은 response에서 쿼리만 따로 추출합니다.
            qSQL = response.choices[0].message.content.replace('\n', ' ')        

        print(system_message)

        print(f"qNL: {qNL}\nqSQL: {qSQL}\n")
        if token:
            print(response.usage)

        return qSQL


if __name__ == "__main__":

    # 전체 실행
    questions = pd.read_csv("presentation/presentation_questions.csv")

    rag = RAG_NO2SQL()

    # pgVector table, issue table 만들기
    # rag.make_VectorDB()
    # rag.make_issue_table()

    for row in questions.iterrows():
        # 자연어 임베딩 해주는 함수
        rag.qNL_to_qSQL(model= 'gpt-4', qNL=row[1].iloc[0])

        