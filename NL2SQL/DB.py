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
            dbname = info['db_info']['dbname']
            user = info['db_info']['user']
            port = info['db_info']['port']
            password = info['db_info']['password']

        self.conn = psy.connect(
            host=localhost,
            dbname=dbname,
            user=user,
            port=port,
            password=password
        )

    def show_schemas(self) -> pd.DataFrame:
        qry = "SELECT schema_name FROM information_schema.schemata;"

        c = self.conn.cursor()
        
        c.execute(qry)
        r = c.fetchall()
        c.close()

        return pd.DataFrame(r, columns=['schema'])

    def show_databases(self) -> pd.DataFrame:
        qry = "SELECT datname FROM pg_database;"

        c = self.conn.cursor()
        
        c.execute(qry)
        r = c.fetchall()
        c.close()

        return pd.DataFrame(r, columns=['database'])

    def show_tables(self) -> pd.DataFrame:
        qry = """
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
              """

        c = self.conn.cursor()
        
        c.execute(qry)
        r = c.fetchall()
        c.close()

        return pd.DataFrame(r, columns=['schema', 'tablename'])

    ## return 이 있는 select
    def select_query(self, qry: str, verbose=False) -> pd.DataFrame:
        if verbose:
            print(f"Print query : \t {qry}")
        
        c = self.conn.cursor()
        try:
            c.execute(qry)
            self.conn.commit()
            r = c.fetchall()
            if len(r) == 0:
                return pd.DataFrame(["None"])
            else:
                return pd.DataFrame(r)

        except Exception as e:
            print(f"Error: {str(e)}")
            c.execute("ROLLBACK;")
            return pd.DataFrame(["Error"])
        finally:
            c.close()

    ## return이 없는 create, insert
    def execute_query(self, qry: str, verbose=False) -> None:
        if verbose:
            print(f"Print query : \t {qry}")

        c = self.conn.cursor()
        try:
            c.execute(qry)
            self.conn.commit()

        except Exception as e:
            print(f"Error: {str(e)}")
            c.execute("ROLLBACK;")

        finally:
            c.close()

    def drop_all_tables(self, verbose=False) -> None:
        if verbose:
            print("Drop all tables in this database")

        c = self.conn.cursor()
        qry = """
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') 
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    -- Print or log the SQL statement being executed for debugging
                    RAISE NOTICE 'Dropping table: %', r.tablename;
                END LOOP;
            END $$;
            """
        try:
            c.execute(qry)
            self.conn.commit()
            if verbose:
                print("All tables dropped successfully.")
        except Exception as e:
            # Print error details for debugging
            print(f"Error code: {e.pgcode}")
            print(f"Error message: {e.pgerror}")
            c.execute("ROLLBACK;")
        finally:
            c.close()


if __name__=="__main__":
    db = MyPostgreSQL()
    db.login()
    # db.drop_all_tables()
    # print(db.show_databases())
    # print(db.show_schemas())

    qry = "SELECT T1.name ,  T2.date_of_treatment FROM Dogs AS T1 JOIN Treatments AS T2 ON T1.dog_id  =  T2.dog_id WHERE T1.breed_code  =  ( SELECT breed_code FROM Dogs GROUP BY breed_code ORDER BY count(*) ASC LIMIT 1 )"
    a = db.select_query(qry)
    print(f"qSQL_result : \n{a}")
    # dataset = pd.read_csv("rawdata/DDL_TABLE.csv")

    # ## test
    # db_id = ""
    # i = 1
    # for row in dataset.iterrows():
    #     current_db_id = row[1]['db_id']
    #     if db_id != current_db_id:
    #         print(i)
    #         i +=1 
    #         print(db.show_tables())
            
    #         db.drop_all_tables()

    #     create_qry = row[1]['CREATE']
    #     insert_qry = row[1]['INSERT']

    #     db.excute_query(create_qry)
    #     db.excute_query(insert_qry)

    #     db_id = current_db_id
        