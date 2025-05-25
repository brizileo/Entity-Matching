
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

# Get configurations
config = configparser.ConfigParser()
config_files = config.read('../config.ini')

db_path = config['DATABASE']['db_path']
jaccard_threshold = config['PARAMETERS']['jaccard_threshold']

# Create a connection to the DuckDB database
conn = duckdb.connect(db_path)

# Get output folder from config
output_folder = config['OUTPUT_FILES']['data_out']
log_path = os.path.join(output_folder, 'tests_control.log')
#%%


with open(log_path, 'w', encoding='utf-8') as log:

    # Check if there are entities that were not matched in tbl_entities_pairs_soft_jaccard
    df = conn.sql("""
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
        """).to_df()
    log.write('Following entities were not matched due to threshold applied to soft jaccard pairs:\n')
    log.write(df.to_string() + '\n')

    # Confusion matrix AI validated pairs
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
    log.write('Confusion matrix of AI validated pairs\n')
    log.write(df.to_string() + '\n')

    # Confusion matrix Jaccard pairs
    df = conn.sql("""
        SELECT 
            SUM(CASE WHEN similarity >= """ +  jaccard_threshold + """ * 1   AND E1.cluster_id = E2.cluster_id THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN similarity >= """ + jaccard_threshold + """ * 1   AND E1.cluster_id != E2.cluster_id THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN similarity < """ +  jaccard_threshold + """ * 1   AND E1.cluster_id = E2.cluster_id THEN 1 ELSE 0 END) AS false_negatives,
            SUM(CASE WHEN similarity < """ +  jaccard_threshold + """ * 1   AND E1.cluster_id != E2.cluster_id THEN 1 ELSE 0 END) AS true_negatives
        FROM tbl_entities_pairs_validated  AS V
        INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
        INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
        """).to_df()
    log.write('Confusion matrix of jaccard pairs with threshold ' + jaccard_threshold + '\n')
    log.write(df.to_string() + '\n')

    # Inspect 10 sample pairs for each category (AI validated pairs)
    categories = [
        ("true_positives", "validation LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id"),
        ("false_positives", "validation LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id"),
        ("false_negatives", "validation NOT LIKE '%Yes%' AND E1.cluster_id = E2.cluster_id"),
        ("true_negatives", "validation NOT LIKE '%Yes%' AND E1.cluster_id != E2.cluster_id"),
    ]

    for label, condition in categories:
        log.write(f"\nSample 10 pairs for category of AI validated pairs: {label}\n")
        sample_df = conn.sql(f"""
            SELECT V.*, E1.cluster_id AS cluster_id_1, E2.cluster_id AS cluster_id_2
            FROM tbl_entities_pairs_validated AS V
            INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
            INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
            WHERE {condition}
            LIMIT 10
        """).to_df()
        log.write(sample_df.to_string() + '\n')

    # Inspect 10 sample pairs for each category (Jaccard pairs)
    jaccard_categories = [
        ("true_positives", f"similarity >= {jaccard_threshold} * 1 AND E1.cluster_id = E2.cluster_id"),
        ("false_positives", f"similarity >= {jaccard_threshold} * 1 AND E1.cluster_id != E2.cluster_id"),
        ("false_negatives", f"similarity < {jaccard_threshold} * 1 AND E1.cluster_id = E2.cluster_id"),
        ("true_negatives", f"similarity < {jaccard_threshold} * 1 AND E1.cluster_id != E2.cluster_id"),
    ]

    for label, condition in jaccard_categories:
        log.write(f"\nSample 10 pairs for category of Jaccard pairs: {label}\n")
        sample_df = conn.sql(f"""
            SELECT V.*, E1.cluster_id AS cluster_id_1, E2.cluster_id AS cluster_id_2
            FROM tbl_entities_pairs_validated AS V
            INNER JOIN tbl_entities AS E1 ON V.entity_id_1 = E1.entity_id
            INNER JOIN tbl_entities AS E2 ON V.entity_id_2 = E2.entity_id
            WHERE {condition}
            LIMIT 10
        """).to_df()
        log.write(sample_df.to_string() + '\n')

# %%
