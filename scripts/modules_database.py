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

    # Create tbl_entities
    conn.execute("DROP TABLE IF EXISTS tbl_entities;")
    conn.execute("""
        CREATE TABLE tbl_entities (
            entity_id           INTEGER PRIMARY KEY
            ,entity_name         VARCHAR NOT NULL
            ,partition_criteria  VARCHAR
            ,cluster_id          INTEGER
        );
    """)

    # Create tbl_entities_tokens
    conn.execute("DROP TABLE IF EXISTS tbl_entities_tokens;")
    conn.execute("""
        CREATE TABLE tbl_entities_tokens (
            entity_id               INTEGER       
            ,entity_name            VARCHAR     
            ,partition_criteria     VARCHAR
            ,cluster_id             INTEGER
            ,token                  VARCHAR
            ,nb_tokens              INTEGER
        );
    """)

     # Create tbl_entities_pairs_jaccard
    conn.execute("DROP TABLE IF EXISTS tbl_entities_pairs_jaccard;")
    conn.execute("""
        CREATE TABLE tbl_entities_pairs_jaccard (
            entity_id_1      INTEGER
            ,entity_name_1   VARCHAR
            ,entity_id_2     INTEGER
            ,entity_name_2   VARCHAR
            ,similarity      FLOAT
        );
    """)   

     # Create tbl_entities_pairs_jaccard
    conn.execute("DROP TABLE IF EXISTS tbl_entities_pairs_soft_jaccard;")
    conn.execute("""
        CREATE TABLE tbl_entities_pairs_soft_jaccard (
            entity_id_1      INTEGER
            ,entity_name_1   VARCHAR
            ,entity_id_2     INTEGER
            ,entity_name_2   VARCHAR
            ,similarity      FLOAT
        );
    """)   

     # Create tbl_entities_pairs_validated
    conn.execute("DROP TABLE IF EXISTS tbl_entities_pairs_validated;")
    conn.execute("""
        CREATE TABLE tbl_entities_pairs_validated (
            entity_id_1      INTEGER
            ,entity_name_1   VARCHAR
            ,entity_id_2     INTEGER
            ,entity_name_2   VARCHAR
            ,similarity      FLOAT
            ,validation      VARCHAR
        );
    """)   

    conn.commit()
    conn.close()











# %%
