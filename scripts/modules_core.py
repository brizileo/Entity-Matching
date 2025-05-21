#%%
import csv
import os
import duckdb
import configparser
import pandas as pd
#%%

def load_entities_from_csv():
    """
    Load entities from a CSV files
    """

    #%%
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    folder_path = config['INPUT_FILES']['entities_list_folder']
    limit_rows = config['PARAMETERS']['limit_rows']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)
    #%%

    # Load files from the folder
    id = 0
    entities = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)  # Skip header row
                for row in reader:
                    #row.append(filename)
                    entities.append([id,' '.join(row[1:4]), row[4] , row[0]]) #recid,givenname,surname,suburb,postcode
                    id += 1

    df = pd.DataFrame(entities, columns=['entity_id', 'entity_name', 'partition_criteria', 'cluster_id'])

    # Persist data to the database
    conn.sql(
    """
        INSERT INTO tbl_entities (entity_id, entity_name, partition_criteria, cluster_id)
        WITH limit_rows AS (
            SELECT DISTINCT cluster_id
            FROM df
            LIMIT """ + str(limit_rows) + """
        )
        SELECT DISTINCT MIN(entity_id), entity_name, partition_criteria, cluster_id
        FROM df
        WHERE cluster_id IN (SELECT cluster_id FROM limit_rows)
        GROUP BY entity_name, partition_criteria, cluster_id
    """
    )

    # Drop cluster where mispelling is on post code so that we can use the post code as partion criteria

    conn.sql('''
        DELETE FROM tbl_entities
        USING (
            SELECT cluster_id
            FROM tbl_entities
            GROUP BY cluster_id
            HAVING COUNT(DISTINCT partition_criteria) > 1
        ) AS to_delete
        WHERE tbl_entities.cluster_id = to_delete.cluster_id
    ''')

    conn.commit()
    conn.close()



def identify_candidate_pairs():
    """
    Tokenise each entity name in the table tbl_entities
    Compute pairwise Jaccard similarity between all entities within the same partition
    Discard all entities for which the Jaccard similarity is below a given jaccard_threshold
    Persist the pairs to a new table named tbl_entities_candidate_pairs
    """
    
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)
    
  
    # Create the table tbl_entities_tokens
    # This table contains the tokenised entity names and the number of tokens for each entity
    # Token splitting is done on space. Leading and trailing special characters are removed
    conn.sql(
    """
    CREATE TABLE tbl_entities_tokens AS
    SELECT *, COUNT(DISTINCT token) OVER (PARTITION BY entity_id) AS nb_tokens
    FROM (
        SELECT
            entity_id           
            ,entity_name         
            ,partition_criteria  
            ,cluster_id     
            ,lower(regexp_replace(word,'^[.,;:!?"''()\\[\\]{}-]+|[.,;:!?"''()\\[\\]{}-]+$','')) AS token
        FROM 
            (
                SELECT
                entity_id           
                ,entity_name         
                ,partition_criteria  
                ,cluster_id         
                ,regexp_split_to_table(entity_name, ' ') AS word
            FROM tbl_entities, 
            ) X 
        ) Y;
    """
    )
    conn.commit()
    conn.close()    

def jaccard_similarity():
    """
    Tokenise each entity name in the table tbl_entities
    Compute pairwise Jaccard similarity between all entities within the same partition
    Discard all entities for which the Jaccard similarity is below a given jaccard_threshold
    Persist the pairs to a new table named tbl_entities_candidate_pairs
    """
    
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    jaccard_threshold = config['PARAMETERS']['jaccard_threshold']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)   

    #Jaccard similarity
    # Create the table tbl_entities_candidate_pairs
    # This table contains the pairs of entities with a Jaccard similarity
    conn.sql(
    """
    CREATE TABLE tbl_entities_pairs_jaccard AS    
    SELECT 
        t1.entity_id    AS entity_id_1,
        t1.entity_name  AS entity_name_1,
        t2.entity_id    AS entity_id_2,
        t2.entity_name  AS entity_name_2,
        COUNT(*)/(t1.nb_tokens + t2.nb_tokens - COUNT(*)) AS jaccard_similarity
    FROM tbl_entities_tokens t1
    INNER JOIN tbl_entities_tokens t2
    ON t1.entity_id < t2.entity_id
    AND t1.partition_criteria = t2.partition_criteria
    AND t1.token = t2.token
    GROUP BY 
        t1.entity_id 
        ,t1.entity_name 
        ,t2.entity_id 
        ,t2.entity_name
        ,t1.nb_tokens
        ,t2.nb_tokens   
    HAVING jaccard_similarity >= """ + str(jaccard_threshold) + """     
    """
    )
    conn.commit()
    conn.close()        

def soft_jaccard_similarity():
    """
    Tokenise each entity name in the table tbl_entities
    Compute pairwise Soft Jaccard similarity between all entities within the same partition
    Discard all entities for which the Jaccard similarity is below a given jaccard_threshold
    Persist the pairs to a new table named tbl_entities_candidate_pairs
    """
    
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    jaccard_threshold = config['PARAMETERS']['jaccard_threshold']
    jaro_winkler_threshold = config['PARAMETERS']['jaro_winkler_threshold']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)   

    #Soft Jaccard with Jaro Winkler
    # Create the table tbl_entities_candidate_pairs
    # This table contains the pairs of entities with a Soft Jaccard similarity

    #%%
    conn.sql(
    """
    CREATE TABLE tbl_entities_pairs_soft_jaccard AS  
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
                similarity,
                ROW_NUMBER() OVER (PARTITION BY entity_id_1, entity_id_2, token_1 ORDER BY similarity DESC) AS rank_t1,
                ROW_NUMBER() OVER (PARTITION BY entity_id_1, entity_id_2, token_2 ORDER BY similarity DESC) AS rank_t2                
            FROM similarity
        ) sub
    ),max_rank AS (
        SELECT 
                entity_id_1,
                entity_name_1,
                entity_id_2,
                entity_name_2,
                nb_tokens_1,
                nb_tokens_2,
                (SUM(CASE WHEN rank_t1 = 1 AND similarity > """ + str(jaro_winkler_threshold) + """  THEN similarity ELSE 0 END)+            
                 SUM(CASE WHEN rank_t2 = 1 AND similarity > """ + str(jaro_winkler_threshold) + """  THEN similarity ELSE 0 END))*0.5 AS Z
        FROM ranked
        GROUP BY 
                entity_id_1,
                entity_name_1,
                entity_id_2,
                entity_name_2,
                nb_tokens_1,
                nb_tokens_2               
    )  
    SELECT 
        entity_id_1,
        entity_name_1,
        entity_id_2,
        entity_name_2,
        Z/(nb_tokens_1 + nb_tokens_2 - Z) AS soft_jaccard_similarity
    FROM max_rank
    WHERE soft_jaccard_similarity >= """ + str(jaccard_threshold) + """     
    """
    )

    conn.commit()
    conn.close()

