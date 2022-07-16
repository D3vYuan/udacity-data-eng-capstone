import psycopg2
from sql_queries import create_table_queries
from drop_tables import drop_tables
from config_loader import load_config

DATABASE_CONFIGURATION_FILE="config.ini"

def create_connection():
    """
    - Load database configuration
    - Creates and connects to the travellers
    - Returns the connection and cursor to travellers
    """
    
    # load database configuration
    config = load_config(DATABASE_CONFIGURATION_FILE)
    database_host=config["postgresql"]["database_host"]
    database_default_name=config["postgresql"]["database_default_name"]
    database_travellers_name=config["postgresql"]["database_travellers_name"]
    database_user=config["postgresql"]["database_user"]
    database_pass=config["postgresql"]["database_pass"]

    # connect to default database
    print(f"Processing: Connecting to {database_default_name}@{database_host}")
    conn = psycopg2.connect(f"host={database_host} dbname={database_default_name} user={database_user} password={database_pass}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create travellers database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS travellers")
    cur.execute("CREATE DATABASE travellers WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to travellers database
    conn = psycopg2.connect(f"host={database_host} dbname={database_travellers_name} user={database_user} password={database_pass}")
    cur = conn.cursor()
    
    return cur, conn

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        print(f"Executing: Creating Table - {query}")
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the travellers database. 
    - Establishes connection with the travellers database and gets
    cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    cur, conn = create_connection()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()