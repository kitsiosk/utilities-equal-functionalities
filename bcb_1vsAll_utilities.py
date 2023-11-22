import psycopg2
import pandas as pd
import os

cache_dir = '/Users/konstantinos/Desktop/Clone Generalization/binary files/' # my Mac
if not os.path.isdir(cache_dir):
    cache_dir = '/home/kkitsi/data' # cluster
    

# Fetches at most N rows for each functionality ID. If a functionality ID has only M<N rows, it fetches them all.
# The rows could be either clones (fetch_clones=True) or non-clones (fetch_clones=False). Resulting table has columns
# code1 | code2 | label (0/1) | functionality_id
def fetch_functionality_data(fetch_clones=True, max_rows_per_functionality=0):
    
    # Database connection parameters
    db_params = {
        "host"    : "localhost",
        "database": "bcb",
        "user"    : "konstantinos",
        "password": "1234"
    }

    # Establish connection
    conn = psycopg2.connect(**db_params)

    if fetch_clones:
        tableName   = "clones"
        label_value = 1
    else:
        tableName   = "false_positives"
        label_value = 0

    # Partition by functionality ID to obtain a row number that resets when functionality changes. Then,
    # fetch all rows with row_num<N
    query = """
    WITH RandomizedRows AS (
      SELECT
        function_id_one, function_id_two, functionality_id, %d as label,
        ROW_NUMBER() OVER (PARTITION BY functionality_id ORDER BY random()) AS row_num
      FROM %s
      WHERE syntactic_type=3    
    )
    SELECT
      f1.text AS code1, f2.text AS code2, R.label, R.functionality_id
    FROM
      RandomizedRows AS R
      INNER JOIN
      pretty_printed_functions AS f1 ON R.function_id_one=f1.function_id
      INNER JOIN 
      pretty_printed_functions AS f2 ON R.function_id_two=f2.function_id
    WHERE R.row_num <= %d;
    """ % (label_value, tableName, max_rows_per_functionality)
    
    data = pd.read_sql_query(query, conn)
    return data

def get_functionality_data(max_rows_per_functionality):
    data_pairs_all_fname       = cache_dir + 'bcb_1vsAll/data_pairs_all_' + str(max_rows_per_functionality) + '.pickle'

    if os.path.isfile(data_pairs_all_fname):
        data_pairs_all = pd.read_pickle(data_pairs_all_fname)
        print("Loaded data_pairs_all_%d.pickle from cache..." % (max_rows_per_functionality))
    else:
        data_true      = fetch_functionality_data(fetch_clones=True , max_rows_per_functionality=max_rows_per_functionality)
        data_false     = fetch_functionality_data(fetch_clones=False, max_rows_per_functionality=max_rows_per_functionality)
        # Avoid creating unbalanced dataset
        n_rows_to_keep = min(len(data_true), len(data_false))
        data_true      = data_true.sample(n_rows_to_keep)
        data_false     = data_false.sample(n_rows_to_keep)
        data_pairs_all = pd.concat([data_true, data_false]).reset_index()
        data_pairs_all.to_pickle(data_pairs_all_fname)

    return data_pairs_all