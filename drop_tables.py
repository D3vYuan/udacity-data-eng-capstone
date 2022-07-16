import psycopg2
from sql_queries import drop_table_queries
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
    database_travellers_name=config["postgresql"]["database_travellers_name"]
    database_user=config["postgresql"]["database_user"]
    database_pass=config["postgresql"]["database_pass"]

    # connect to travellers database
    conn = psycopg2.connect(f"host={database_host} dbname={database_travellers_name} user={database_user} password={database_pass}")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        print(f"Executing: Dropping Table - {query}")
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the travellers database.     
    - Establishes connection with the travellers database and gets
    cursor to it.  
    - Drops all the tables.  
    - Finally, closes the connection. 
    """
    cur, conn = create_connection()
    
    drop_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()