
# """
# This checks the results of the soft Jaccard similarity and AI validation steps.
# It generates a log file with the results of the tests.
# This script should be run after the main.py script. 
# It assume that the database has been created and the data has been loaded.
# It also assumes that the soft Jaccard similarity and AI validation steps have been run.
# These controls can be executed only in testing phase when the true pairs are known a priori. 
# """

#%%
import duckdb
import configparser
import os
import yaml
def report():

    # Get configurations
    config = configparser.ConfigParser()
    config_files = config.read('../config.ini')

    db_path = config['DATABASE']['db_path']
    jaccard_threshold = config['PARAMETERS']['jaccard_threshold']
    min_token_length = config['PARAMETERS']['tokens_min_length']

    # Create a connection to the DuckDB database
    conn = duckdb.connect(db_path)

    # Get output folder from config
    output_folder = config['OUTPUT_FILES']['data_out']
    log_path = os.path.join(output_folder, 'tests_control.log')


    with open(log_path, 'w', encoding='utf-8') as log:

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
        log.write('==' * 50 + '\n')   
        log.write('Check if the the jaccard similarity threshold = '+jaccard_threshold+' and min token lenngth = '+min_token_length+' excludes true pairs:\n')    
        log.write('==' * 50 + '\n')            
        log.write(df.to_string() + '\n')

        df = conn.sql("""
            SELECT DISTINCT tp.*
            FROM tbl_entities_true_pairs AS tp
            LEFT JOIN tbl_entities_pairs_soft_jaccard AS sj
            ON tp.entity_id_1 = sj.entity_id_1 AND tp.entity_id_2 = sj.entity_id_2 OR
                tp.entity_id_1 = sj.entity_id_2 AND tp.entity_id_2 = sj.entity_id_1
            WHERE sj.entity_id_1 IS NULL AND sj.entity_id_2 IS NULL
            """).to_df()
        log.write('Following entities were not matched due to threshold applied to soft jaccard pairs:\n')
        log.write(df.to_string() + '\n')
        
        # Confusion matrix AI validated pairs
        log.write('--' * 50 + '\n')   
        log.write('Confusion matrix of AI validated pairs\n')
        log.write('--' * 50 + '\n')   
        df = conn.sql("""
            SELECT 
                SUM(CASE WHEN validation LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id THEN 1 ELSE 0 END) AS true_positives,
                SUM(CASE WHEN validation LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id THEN 1 ELSE 0 END) AS false_positives,
                SUM(CASE WHEN validation NOT LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id THEN 1 ELSE 0 END) AS false_negatives,
                SUM(CASE WHEN validation NOT LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id THEN 1 ELSE 0 END) AS true_negatives
            FROM tbl_entities_pairs_validated  AS V
            INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
            INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
            """).to_df()
        log.write(df.to_string() + '\n')

        # Confusion matrix Jaccard pairs
        log.write('--' * 50 + '\n')
        log.write('Confusion matrix of Jaccard pairs with threshold ' + jaccard_threshold + '\n')
        log.write('--' * 50 + '\n')
        df = conn.sql("""
            SELECT 
                SUM(CASE WHEN E1.cluster_id = E2.cluster_id THEN 1 ELSE 0 END) AS true_positives,
                SUM(CASE WHEN E1.cluster_id != E2.cluster_id THEN 1 ELSE 0 END) AS false_positives,
                0 AS false_negatives,
                0 AS true_negatives
            FROM tbl_entities_pairs_validated  AS V
            INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
            INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
            """).to_df()
        log.write(df.to_string() + '\n')

        log.write('==' * 50 + '\n')
        log.write('Inspect examples of pairs\n')
        log.write('==' * 50 + '\n')
        
        # Inspect 10 sample pairs for each category (AI validated pairs)
        categories = [
            ("true_positives", "validation LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id"),
            ("false_positives", "validation LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id"),
            ("false_negatives", "validation NOT LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id"),
            ("true_negatives", "validation NOT LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id"),
        ]

        for label, condition in categories:
            log.write(f"\nSample 10 pairs to inspect AI validated pairs: {label}\n")
            sample_df = conn.sql(f"""
                SELECT V.*, E1.cluster_id AS cluster_id_1, E2.cluster_id AS cluster_id_2
                FROM tbl_entities_pairs_validated AS V
                INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
                INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
                WHERE {condition}
                LIMIT 10
            """).to_df()
            log.write(sample_df.to_string() + '\n')

        log.write('--' * 50 + '\n')

        # Inspect 10 sample pairs for each category (Jaccard pairs)
        jaccard_categories = [
            ("true_positives", "E1.cluster_id = E2.cluster_id"),
            ("false_positives","E1.cluster_id != E2.cluster_id")
        ]

        for label, condition in jaccard_categories:
            log.write(f"\nSample 10 pairs to inspect Jaccard pairs: {label}\n")
            sample_df = conn.sql(f"""
                SELECT V.*, E1.cluster_id AS cluster_id_1, E2.cluster_id AS cluster_id_2
                FROM tbl_entities_pairs_validated AS V
                INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
                INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
                WHERE {condition}
                LIMIT 10
            """).to_df()
            log.write(sample_df.to_string() + '\n')


        with open('../prompts_library.yaml', 'r', encoding='utf-8') as f:
            prompt_library = yaml.safe_load(f)
            prompt = prompt_library['entity_match_review']['prompt']

            log.write('==' * 50 + '\n')
            log.write('PROMPT\n')
            log.write('==' * 50 + '\n')    
            log.write(prompt)

    # %%
