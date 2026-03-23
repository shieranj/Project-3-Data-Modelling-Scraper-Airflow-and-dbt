import psycopg2
import pandas as pd
from psycopg2 import sql, extras
from datetime import datetime


class PostgresConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def get_psycopg2_conn(self):
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL via psycopg2")
        except Exception as e:
            print(f"Error connecting psycopg2-postgres: {e}")

    def table_exist(self, table_name):
        """
        returns Boolean, True if table exists and vice versa
        """
        query = """
        SELECT EXISTS(
            SELECT FROM information_schema.tables
                WHERE table_name = %s
        );
        """
        try:
            self.cursor.execute(query, (table_name,))
            exists = self.cursor.fetchone()[0]
            return exists
        except Exception as e:
            print("Error checking table existence:", e)
            return False
    
    def create_table(self, table_name, df):
        """
        Docstring for create_table
        
        :param table_name: new table name 
        :param df: DataFrame
        """
        type_mapping = {
        'object': 'VARCHAR(255)',
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'date': 'DATE',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64': 'TIMESTAMP',
    }
        column_defs = []
        for col in df.columns:
            col_type = str(df[col].dtype)

            if col.endswith('day') or col.endswith('_date'):
                postgres_type = 'DATE'

            elif col.endswith('_at'):
                postgres_type = 'TIMESTAMP'
            
            elif col.endswith('amount') or col.endswith('_price'):
                postgres_type = 'REAL'

            else:
                #default is text if none are matching
                postgres_type = type_mapping.get(col_type.lower(), 'TEXT')

            column_defs.append(f"{col} {postgres_type}")

        columns_sql = ",\n        ".join(column_defs)

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
        );
        """
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print(f"Successfully created table {table_name}")

        except Exception as e:
            print(f"Error creating table {table_name}, {e}")
            raise
    
    def alter_table(self, table_name, primary_key, foreign_key = None, fk_source_key=None, fk_source_table=None):
            """
            Docstring for alter_table
            
            :param table_name: table name to alter
            :param primary_key: primary key column name
            :param foreign_key: foreign key column name
            :param fk_source_key: foreign key reference column name
            :param fk_source_table: foreign key reference table name
            """

            try:
                query = f"""
                ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});
                """
                self.cursor.execute(query)
                self.conn.commit()
                print(f"Added primary key {primary_key} to table {table_name}")
            except Exception as e:
                self.conn.rollback()
                print(f"Error adding primary key to {table_name}: {e}")
                raise

            if foreign_key is not None:
                try:
                    query = f"""
                    ALTER TABLE {table_name} 
                    ADD CONSTRAINT fk_{foreign_key} 
                    FOREIGN KEY ({foreign_key}) 
                    REFERENCES {fk_source_table}({fk_source_key});
                    """
                    self.cursor.execute(query)
                    self.conn.commit()
                    print(f"Added foreign key {foreign_key} to table {table_name} referencing {fk_source_table}({fk_source_key})")
                except Exception as e:
                    self.conn.rollback()
                    print(f"Error adding foreign key to {table_name}: {e}")
                    raise

    def insert_data(self,table_name, df, batch_size=100):
        """
        Docstring for insert_data
        
        :param table_name: table name
        :param df: DataFrame to insert
        :param batch_size: batch size default 100
        """
        if df.empty:
            print(f"Dataframe is empty, no data to insert to {table_name}!")
            raise

        df_copy = df.copy()

        df_copy.columns = df_copy.columns.str.lower()

        try:
            columns = list(df_copy.columns)
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier,columns)),
            )

            #itertuples is faster than looping with iterrows 
            records = list(df_copy.itertuples(index = False, name = None))

            for start in range(0, len(records), batch_size):
                batch = records[start: start + batch_size]
                extras.execute_values(self.cursor, query, batch, page_size = batch_size)
            
            self.conn.commit()
            print(f"Inserted {len(records)} rows into {table_name}")
            return True
        
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting data into {table_name}: {e}")
            raise

    def load_dataframe(self, table_name, df, primary_key, if_exists="append", foreign_key = None, fk_source_key=None, fk_source_table=None):
        try:
            table_exists = self.table_exist(table_name)

            if if_exists=="replace":
                if table_exists:
                    self.cursor.execute(f"DROP TABLE IF EXISTS public.{table_name}")
                    self.conn.commit()
                    print(f"Dropped existing table {table_name}")
                
                self.create_table(table_name, df)
                self.alter_table(table_name, primary_key, foreign_key, fk_source_key, fk_source_table)
                self.insert_data(table_name, df)

            elif if_exists=="append":
                if not table_exists:
                    self.create_table(table_name, df)
                    self.alter_table(table_name, primary_key, foreign_key, fk_source_key, fk_source_table)

                self.insert_data(table_name, df)
            
            elif if_exists=="fail":
                if table_exists:
                    raise ValueError(f"Table {table_name} already exists")
                
                self.create_table(table_name, df)
                self.alter_table(table_name, primary_key, foreign_key, fk_source_key, fk_source_table)
                self.insert_data(table_name, df)

        except Exception as e:
            print(f"Error loading dataframes to PostgreSQL, {e}")
            raise

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()

            if self.conn:
                self.conn.close()
            
            print("PostgreSQL Connection Closed")

        except Exception as e:
            print(f"Error closing Postgres Connection!{e}")
