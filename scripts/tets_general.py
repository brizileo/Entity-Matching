#%%
import duckdb
import configparser


# Get configurations
config = configparser.ConfigParser()
config_files = config.read('../config.ini')

db_path = config['DATABASE']['db_path']


# Create a connection to the DuckDB database
conn = duckdb.connect(db_path)

############################

#%%

df = conn.sql('''
        SELECT *
        FROM tbl_entities_pairs
        ORDER BY jaccard_similarity DESC
    ''').to_df()
#%%
df = conn.sql('''
        SELECT COUNT(*) AS total
        FROM tbl_entities
        WHERE cluster_id IN 
            (
            SELECT cluster_id
            FROM tbl_entities
            GROUP BY cluster_id
            HAVING COUNT(DISTINCT partition_criteria) > 1
            ) 
    ''').to_df()

#%%
df = conn.sql('''
        SELECT COUNT(DISTINCT entity_name || partition_criteria || CAST(cluster_id AS VARCHAR)) AS count
        FROM tbl_entities
    ''').to_df()

#%%
df = conn.sql('''
        SELECT DISTINCT MIN(entity_id), entity_name, partition_criteria, cluster_id
        FROM tbl_entities
        GROUP BY entity_name, partition_criteria, cluster_id;
    ''').to_df()

#%%
df = conn.sql('''
    SELECT *
    FROM tbl_entities
    WHERE NOT (CAST(cluster_id AS VARCHAR) ~ '^[0-9]+$')
    LIMIT 10;
    ''').to_df()


#%%
conn.commit()
conn.close()    

#%%
print(3896319-411913) #3484406

3484406

# %%

# Get configurations
conn.sql(
    """
DROP TABLE IF EXISTS tbl_entities_pairs;    
    """
    )

# %%
conn.commit()
# %%
conn.close()
# %%
conn.sql(
    """
    SELECT *   
    FROM tbl_entities_tokens
    LIMIT 10 
    """
    )
# %%
