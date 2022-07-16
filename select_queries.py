import psycopg2
from sql_queries import select_table_queries, select_table_descriptions
from config_loader import load_config
import argparse

DATABASE_CONFIGURATION_FILE="config.ini"

my_parser = argparse.ArgumentParser(description='Run a use case query')
my_parser.add_argument('-c', '--use-case', action='store', \
        choices=range(1, len(select_table_queries) + 1), \
        type=int, metavar='USE_CASE_ID', \
        required=True)

def create_connection():
    """
    - Load database configuration
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

def select_query(cur, conn, select_idx):
    """
    Select query based on the index in `select_table_queries` list. 
    """

    select_query = select_table_queries[select_idx - 1]
    select_query_description = select_table_descriptions[select_idx - 1]

    print(f"Executing: Selecting #{select_idx} - {select_query_description}")
    print(f"Executing: Selecting #{select_idx} - {select_query}")
    cur.execute(select_query)
    result=cur.fetchall()
    print(f"Executing: found [{len(result)}] records")
    for row in result:
        print(row)
    print("--")
    conn.commit()

def main():
    """  
    - Establishes connection with the travellers database and gets
    cursor to it.  
    - Select query based on the user choice. 
    - Finally, closes the connection. 
    """
    args = my_parser.parse_args()
    selected_use_case = args.use_case
    cur, conn = create_connection()
    
    select_query(cur, conn, selected_use_case)

    conn.close()


if __name__ == "__main__":
    main()