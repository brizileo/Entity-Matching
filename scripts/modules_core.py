#%%
import csv
import os
import duckdb
import configparser
import pandas as pd
import yaml
from ollama import generate
from tqdm import tqdm
#%%

def load_entities_from_csv():
    """
    Load entities from a CSV files into the tbl_entities table in the DuckDB database.
    This fucntion need to be adapted to the structure of the CSV files when used in production.
    The creation of the true pairs table is only possible for testing purposes when the entity clusters are known a priori.
    """

    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    folder_path = config['INPUT_FILES']['entities_list_folder']
    limit_rows = config['PARAMETERS']['limit_rows']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)

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
                    entities.append([id,' '.join(row[1:5]), 'partition1' , row[0]]) #recid,givenname,surname,suburb,postcode
                    id += 1

    df = pd.DataFrame(entities, columns=['entity_id', 'entity_name', 'partition_criteria', 'cluster_id'])

    # Persist data to the database
    conn.sql(
    """
        INSERT INTO tbl_entities (entity_id, entity_name, partition_criteria, cluster_id)
        WITH limit_rows AS (
            SELECT DISTINCT cluster_id
            FROM df
            ORDER BY 1 ASC            
            LIMIT """ + str(limit_rows) + """
        )
        SELECT DISTINCT MIN(entity_id), entity_name, partition_criteria, cluster_id
        FROM df
        WHERE cluster_id IN (SELECT cluster_id FROM limit_rows)
        GROUP BY entity_name, partition_criteria, cluster_id

    """
    )

    # Create table of true pairs (only possible for testing purposes when the entity clusters are known a priori)
    conn.sql(
    """
        INSERT INTO tbl_entities_true_pairs(entity_id_1, entity_name_1, entity_id_2, entity_name_2, cluster_id)
        SELECT DISTINCT
            t1.entity_id AS entity_id_1,
            t1.entity_name AS entity_name_1,
            t2.entity_id AS entity_id_2,
            t2.entity_name AS entity_name_2,
            t1.cluster_id
        FROM tbl_entities t1
        INNER JOIN tbl_entities t2
        ON t1.cluster_id = t2.cluster_id
        AND t1.entity_id < t2.entity_id
        WHERE t1.cluster_id IN (SELECT cluster_id FROM tbl_entities)
    """
    )


    conn.commit()
    conn.close()



def tokenize():
    """
    Tokenise each entity name in the table tbl_entities
    """
    
    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    min_token_length = config['PARAMETERS']['tokens_min_length']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)
    
  
    # Create the table tbl_entities_tokens
    # This table contains the tokenised entity names and the number of tokens for each entity
    # Token splitting is done on space. Leading and trailing special characters are removed
    conn.sql(
    """
    INSERT INTO tbl_entities_tokens (entity_id, entity_name, partition_criteria, cluster_id, token, nb_tokens)
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
        ) Y
    WHERE LENGTH(token) > """ + min_token_length + """;
    """
    )
    conn.commit()
    conn.close()    



def jaccard_similarity():
    """
    Compute pairwise Jaccard similarity between all entities within the same partition
    Discard all entities for which the Jaccard similarity is below a given jaccard_threshold
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

    entities_list = conn.sql('''
    SELECT DISTINCT entity_id FROM tbl_entities
    ''').to_df()

    total_pairs = len(entities_list)
    pbar = tqdm(total=total_pairs, desc="Computing jaccard similarity of candidate pairs")

    for current_entity in entities_list['entity_id']:
        conn.sql(
        """
        INSERT INTO tbl_entities_pairs_jaccard (entity_id_1,entity_name_1,entity_id_2,entity_name_2,similarity)    
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
        WHERE t1.entity_id = """ + str(current_entity) + """
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
        pbar.update(1)

    pbar.close()
    conn.close()        



def soft_jaccard_similarity():
    """
    Compute pairwise Soft Jaccard similarity between all entities within the same partition
    Discard all entities for which the soft Jaccard similarity is below a given jaccard_threshold
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

    entities_list = conn.sql('''
    SELECT DISTINCT entity_id FROM tbl_entities
    ''').to_df()

    total_pairs = len(entities_list)
    pbar = tqdm(total=total_pairs, desc="Computing soft jaccard similarity of candidate pairs")

    for current_entity in entities_list['entity_id']:    

        conn.sql(
        """
        INSERT INTO tbl_entities_pairs_soft_jaccard (entity_id_1,entity_name_1,entity_id_2,entity_name_2,similarity)  
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
            WHERE t1.entity_id = """ + str(current_entity) + """
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
        pbar.update(1)

    pbar.close()
    conn.close()



def pairs_validation(similarity_model='soft_jaccard'):
    """
    Validate the pairs of entities using the Ollama API
    Takes as input the similarity model to use e.g. either 'jaccard' or 'soft_jaccard'
    """

    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    min_token_length = config['PARAMETERS']['tokens_min_length']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)   
    cursor = conn.cursor() 
    
    # Read and parse the prompt_library.yaml file
    with open('../prompts_library.yaml', 'r', encoding='utf-8') as f:
        prompt_library = yaml.safe_load(f)
    
    # Get total number of pairs for progress bar
    total_pairs = cursor.execute('''
        SELECT COUNT(*) FROM tbl_entities_pairs_''' + similarity_model + '''
    ''').fetchone()[0]
    
    # Loop through the pairs of entities
    pairs_list = cursor.execute('''
    SELECT DISTINCT entity_id_1,entity_name_1,entity_id_2,entity_name_2,similarity
    FROM tbl_entities_pairs_''' + similarity_model + '''
    ''')

    row = pairs_list.fetchone()
    pbar = tqdm(total=total_pairs, desc="Validating candidate pairs with AI model")

    while row is not None:
        entity_id_1 = row[0]
        entity_name_1 = row[1]
        entity_id_2 = row[2]
        entity_name_2 = row[3]
        similarity = row[4]

        # Prepare the prompt
        prompt = prompt_library['entity_match_review']['prompt']
        prompt = prompt.replace('[INSERT STRING A]', entity_name_1)
        prompt = prompt.replace('[INSERT STRING B]', entity_name_2)
        prompt = prompt.replace('[MIN_LENGTH]', min_token_length)
        
        # Call the Ollama API
        response = generate('phi4-mini', prompt)

        conn.execute('''
            INSERT INTO tbl_entities_pairs_validated (entity_id_1, entity_name_1, entity_id_2, entity_name_2, similarity, validation)
            VALUES(?,?, ?, ?, ?, ?)
        ''', [entity_id_1, entity_name_1, entity_id_2, entity_name_2, similarity, response['response']])
  
        row = pairs_list.fetchone()
        pbar.update(1)
    pbar.close()

    conn.commit()
    conn.close()


# %%
