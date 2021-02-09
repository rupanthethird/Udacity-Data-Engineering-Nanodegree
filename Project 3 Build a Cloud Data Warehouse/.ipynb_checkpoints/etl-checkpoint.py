import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    execute SQL queries that are listed in copy_table_queries.
    
    INPUTS:
    * cur the cursor variable
    * conn connection to database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    execute SQL queries that are listed in insert_table_queries.
    
    INPUTS:
    * cur the cursor variable
    * conn connection to database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - connect to PostgreSQL using the information written in 'CLUSTER' section in 'dwh.cfg'.
    - execute load_staging_tables and insert_tables, and close the connection.
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()