#%%
import duckdb
import configparser
import os


def setup_database():
    """
    This function sets up the database by creating a connection to the DuckDB database.
    It generates the tables neeeded for the application and loads the data from the CSV files. 
    """
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')
    

    db_path = config['DATABASE']['db_path']

    # Delete the database file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)

    # Drop the table if it exists and recreate it
    conn.execute("DROP TABLE IF EXISTS tbl_entities;")
    conn.execute("""
        CREATE TABLE tbl_entities (
            entity_id           INTEGER PRIMARY KEY,
            entity_name         VARCHAR NOT NULL,
            partition_criteria  VARCHAR,
            cluster_id          INTEGER
        );
    """)

    conn.commit()
    conn.close()











# %%
