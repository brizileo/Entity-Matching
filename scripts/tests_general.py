#%%
import csv
import os
import duckdb
import configparser
import pandas as pd
import yaml
from ollama import generate


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

#%%

df = conn.sql('''
    SELECT*
    FROM 
        (
        SELECT entity_id_1, entity_name_1, entity_id_2, entity_name_2
        FROM tbl_entities_pairs_jaccard   
        UNION 
        SELECT entity_id_2, entity_name_2, entity_id_1, entity_name_1
        FROM tbl_entities_pairs_jaccard 
        ) AS t   
    EXCEPT
    SELECT*
    FROM 
        (
        SELECT entity_id_1, entity_name_1, entity_id_2, entity_name_2
        FROM tbl_entities_pairs_soft_jaccard   
        UNION 
        SELECT entity_id_2, entity_name_2, entity_id_1, entity_name_1
        FROM tbl_entities_pairs_soft_jaccard  
        ) AS t                         
    ''').to_df()
# %%

df = conn.sql('''


    SELECT *
    FROM tbl_entities_pairs_jaccard                     
    ''').to_df()

# %%


#%%

df = conn.sql('''
    SELECT*
    FROM 
        (
        SELECT entity_id_1, entity_name_1, entity_id_2, entity_name_2
        FROM tbl_entities_pairs_soft_jaccard   
        UNION 
        SELECT entity_id_2, entity_name_2, entity_id_1, entity_name_1
        FROM tbl_entities_pairs_soft_jaccard
        ) AS t   
    EXCEPT
    SELECT*
    FROM 
        (
        SELECT entity_id_1, entity_name_1, entity_id_2, entity_name_2
        FROM tbl_entities_pairs_jaccard 
        UNION 
        SELECT entity_id_2, entity_name_2, entity_id_1, entity_name_1
        FROM tbl_entities_pairs_jaccard
        ) AS t                         
    ''').to_df()
#%%

df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_soft_jaccard  A
    LEFT JOIN tbl_entities_pairs_jaccard B
    ON A.entity_id_1 = B.entity_id_1
    AND A.entity_id_2 = B.entity_id_2
    WHERE B.entity_id_1 IS NULL
    ORDER BY A.soft_jaccard_similarity DESC              
    ''').to_df()
#%%

df = conn.sql('''
   WITH similarity AS (
        SELECT 
            t1.entity_id    AS entity_id_1,
            t1.entity_name  AS entity_name_1,
            t2.entity_id    AS entity_id_2,
            t2.entity_name  AS entity_name_2,
            t1.nb_tokens    AS nb_tokens_1,
            t2.nb_tokens    AS nb_tokens_2, 
            t1.token        AS token_1,
            t2.token        AS token_2,
            jaro_winkler_similarity(t1.token, t2.token) AS similarity
        FROM tbl_entities_tokens t1
        INNER JOIN tbl_entities_tokens t2
        ON t1.entity_id < t2.entity_id
        AND t1.partition_criteria = t2.partition_criteria
        WHERE entity_id_1 = 1644406 AND entity_id_2 = 2028315                
        )
    ,ranked AS (
        SELECT *
        FROM 
        (
            SELECT 
                entity_id_1,
                entity_name_1,
                entity_id_2,
                entity_name_2,
                nb_tokens_1,
                nb_tokens_2,
                token_1,
                token_2,
                similarity,
                ROW_NUMBER() OVER (PARTITION BY entity_id_1, entity_id_2, token_1 ORDER BY similarity DESC) AS rank
            FROM similarity
        ) sub
        WHERE rank = 1
       )
    SELECT *
    FROM ranked
    ''').to_df()
#%%
df = conn.sql('''
    SELECT jaro_winkler_similarity('la', 'melisa') AS similarity
    ''').to_df()

#%%
df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_soft_jaccard
    WHERE soft_jaccard_similarity > 1
    ''').to_df()
# %%

#%%
df = conn.sql('''
    SELECT B.*,A1.cluster_id AS cluster_id1, A2.cluster_id AS cluster_id2
    FROM tbl_entities_pairs_soft_jaccard B
    LEFT JOIN tbl_entities A1
    ON B.entity_id_1 = A1.entity_id
    LEFT JOIN tbl_entities A2
    ON B.entity_id_2 = A2.entity_id
    ORDER BY similarity ASC
    LIMIT 1000
    ''').to_df()
#%%
df = conn.sql('''
    SELECT *
    FROM tbl_entities A1
    WHERE A1.cluster_id IN (7754527)
    
    ''').to_df()

#%%
df = conn.sql('''
    SELECT cluster_id
    FROM tbl_entities A1
    GROUP BY cluster_id
    HAVING COUNT(DISTINCT entity_id) > 1
    
    ''').to_df()
#%%
df = conn.sql('''
    SELECT entity_id FROM tbl_entities
    EXCEPT
    SELECT *
    FROM (
         SELECT entity_id_1 AS entity_id FROM tbl_entities_pairs_soft_jaccard
         UNION
         SELECT entity_id_2 AS entity_id FROM tbl_entities_pairs_soft_jaccard
         ) AS t     
    ''').to_df()

#%%
df = conn.sql('''

         SELECT entity_id_1 AS entity_id FROM tbl_entities_pairs_soft_jaccard
         UNION
         SELECT entity_id_2 AS entity_id FROM tbl_entities_pairs_soft_jaccard
    
    ''').to_df()

#%%
df = conn.sql('''

SELECT * FROM tbl_entities
    ''').to_df()

#%%
df = conn.sql('''

SELECT * FROM tbl_entities_pairs_soft_jaccard
    ''').to_df()

#%%
df = conn.sql('''

SELECT * FROM tbl_entities_pairs_validated
    ''').to_df()
# %%
#%%
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
  
#%%
conn.close()

##############OLLAMA TESTS######################
# %%

# false negatives
df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_validated    
    WHERE validation LIKE '%Yes%' AND similarity < 0.6  
    ''').to_df()

# %%

# true negatives
df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_validated    
    WHERE validation NOT LIKE '%Yes%' AND similarity < 0.6  
    ''').to_df()

# %%

# true positives
df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_validated    
    WHERE validation LIKE '%Yes%' AND similarity > 0.8 
    ''').to_df()

# %%

# false positives
df = conn.sql('''
    SELECT *
    FROM tbl_entities_pairs_validated    
    WHERE validation NOT LIKE '%Yes%' AND similarity > 0.8
    ''').to_df()



# %%


df = conn.sql('''
    SELECT DISTINCT validation
    FROM tbl_entities_pairs_validated     
    ''').to_df()


# %%


# Check if there are pairs that were not matched in tbl_entities_pairs_soft_jaccard
df = conn.sql("""
    SELECT COUNT(*)
    FROM tbl_entities_true_pairs
    """).to_df()
#%%
# Check if there are pairs that were not matched in tbl_entities_pairs_soft_jaccard
df = conn.sql("""
    SELECT    COUNT(*) AS total_pairs
              ,COUNT(CASE WHEN sj.entity_id_1 IS NOT NULL THEN sj.entity_id_1 END) matched_pairs
              ,total_pairs-matched_pairs AS unmatched_pairs
              ,COUNT(CASE WHEN sj.entity_id_1 IS NOT NULL THEN sj.entity_id_1 END )/COUNT(*) AS ratio
    FROM tbl_entities_true_pairs AS tp
    LEFT JOIN tbl_entities_pairs_soft_jaccard AS sj
    ON tp.entity_id_1 = sj.entity_id_1 AND tp.entity_id_2 = sj.entity_id_2 OR
        tp.entity_id_1 = sj.entity_id_2 AND tp.entity_id_2 = sj.entity_id_1
    """).to_df()
# %%
