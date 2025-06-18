import psycopg
import os
from dotenv import load_dotenv


class PostgresSQL:
    def __init__(self):
        #self.type = type
        #self.prompt = SQLPrompt
        load_dotenv()
        self.db_user = os.getenv("DB_USER")
        self.db_port = os.getenv("DB_PORT")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_name = os.getenv("DB_DBNAME")
        self.db_host = os.getenv("DB_ADDR")


    def conn(self):
        conn = psycopg.connect(
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
        return conn
    
    def FetchAllData(self, table):
        conn = self.conn()

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table}")
            all_data = cur.fetchall()
        
        return all_data
    
    def FetchSpecificData(self, table,condition):
        '''condition must be in format :WHERE id <5'''
        conn = self.conn()

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} {condition}")
            all_data = cur.fetchmany()
        
        return all_data
    
    def InsertData(self, table, data: dict):
        """
        Expecting: {"currency": "USD", "forecast": "280K"}
        Returns:
            200 on success,
            400 or other code on failure.
        """
        try:
            conn = self.conn()
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            values = tuple(data.values())

            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            with conn.cursor() as cur:
                cur.execute(query, values)

            conn.commit()
            return 200  

        except Exception as e:
            print(f"error as {e}")
            return 400