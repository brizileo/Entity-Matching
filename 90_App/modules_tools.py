#%%
import csv
import os
import duckdb
import configparser
import pandas as pd

def load_entities_from_csv():
    """
    Load entities from a CSV files
    """
    # Define the folder path containing the CSV files
    folder_path = '../00_Data_In/sample/'
    entities = []

    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)

    # Load files from the folder
    id = 0
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)  # Skip header row
                for row in reader:
                    #row.append(filename)
                    entities.append([id,' '.join(row[1:5]), '' , row[0]]) #recid,givenname,surname,suburb,postcode
                    id += 1

    df = pd.DataFrame(entities, columns=['entity_id', 'entity_name', 'partition_criteria', 'cluster_id'])

    # Persist data to the database
    conn.sql('''
        INSERT INTO tbl_entities (entity_id, entity_name, partition_criteria, cluster_id)
        SELECT DISTINCT MIN(entity_id), entity_name, partition_criteria, cluster_id
        FROM df
        GROUP BY entity_name, partition_criteria, cluster_id;
    ''')

    conn.commit()
    conn.close()

    


#%%