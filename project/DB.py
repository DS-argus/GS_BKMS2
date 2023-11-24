import psycopg2 as psy
import pandas as pd
import json

class MyPostgreSQL:
    """
    psycopg2을 이용한 PostgreSQL class
    """

    @classmethod
    def __get_instance(cls):
        """
        Singleton 구현
        """
        return cls.__instance


    @classmethod
    def instance(cls, *args, **kwargs):
        """
        Singleton 구현
        """
        cls.__instance = cls(*args, **kwargs)
        cls.instance = cls.__get_instance

        return cls.__instance


    def __init__(self):
        pass


    def login(self):
        with open("info.json", 'r') as file:
            info = json.load(file)
            localhost = info['db_info']['localhost']
            user = info['db_info']['user']
            port = info['db_info']['port']
            password = info['db_info']['password']

        self.conn = psy.connect(
            host=localhost,
            user=user,
            port=port,
            password=password
        )


    def excute_query(self, qry: str) -> pd.DataFrame:
        print(f"Print query : \t {qry}")

        c = self.conn.cursor()
        c.execute(qry)
        r = c.fetchall()
        c.close()

        return pd.DataFrame(r)

    def drop_all_tables(self) -> None:
        print("Drop all tables in this database")

        c = self.conn.cursor()
        qry = """
                DO $$
                DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """

        c.execute(qry)
        c.close()

if __name__=="__main__":
    db = MyPostgreSQL()
    db.login()
    
    qry = "SELECT datname FROM pg_database;"
    print(db.excute_query(qry))