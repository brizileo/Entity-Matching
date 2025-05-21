#%%
import duckdb
import configparser


# Get configurations
config = configparser.ConfigParser()
config_files = config.read('../config.ini')

db_path = config['DATABASE']['db_path']

# Create a connection to the DuckDB database
conn = duckdb.connect(db_path)

df = conn.sql('''
    SELECT entity_id
    FROM tbl_entities
    WHERE cluster_id IN
        (
        SELECT cluster_id
        FROM tbl_entities
        GROUP BY cluster_id
        HAVING COUNT(DISTINCT entity_id) > 1
        ) 
    EXCEPT
    SELECT *
    FROM (
         SELECT entity_id_1 AS entity_id FROM tbl_entities_pairs_soft_jaccard
         UNION
         SELECT entity_id_2 AS entity_id FROM tbl_entities_pairs_soft_jaccard
         ) AS t               
    ''').to_df()
print('Following entities were not matched')
print(df)
# %%
