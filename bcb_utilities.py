import pandas as pd
import os

cache_dir = '/Users/konstantinos/local-desktop/Clones paper replication package/datasets/bcb_v2_sampled_bf/' # my Mac
if not os.path.isdir(cache_dir):
    cache_dir = '/home/kkitsi/data/equal_func/' # cluster
    

# Fetches at most N rows for each functionality ID. If a functionality ID has only M<N rows, it fetches them all.
# The rows could be either clones (fetch_clones=True) or non-clones (fetch_clones=False). Resulting table has columns
# code1 | code2 | label (0/1) | functionality_id
def fetch_functionality_data(fetch_clones=True, max_rows_per_functionality=0):
    import psycopg2

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
    SELECT setseed(0.42);
    
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
    data_pairs_all_fname = cache_dir + 'data_pairs_all_' + str(max_rows_per_functionality) + '.pickle'

    if os.path.isfile(data_pairs_all_fname):
        data_pairs_all = pd.read_pickle(data_pairs_all_fname)
        print("Loaded data_pairs_all_%d.pickle from cache..." % (max_rows_per_functionality))
    else:
        data_true      = fetch_functionality_data(fetch_clones=True , max_rows_per_functionality=max_rows_per_functionality)
        data_false     = fetch_functionality_data(fetch_clones=False, max_rows_per_functionality=max_rows_per_functionality)
        
        # Avoid creating unbalanced dataset
        # n_rows_to_keep = min(len(data_true), len(data_false))
        # data_true      = data_true.sample(n_rows_to_keep)
        # data_false     = data_false.sample(n_rows_to_keep)
        # data_pairs_all = pd.concat([data_true, data_false]).reset_index()


        # Find common functionality_ids
        common_functionality_ids = set(data_true['functionality_id']).intersection(set(data_false['functionality_id']))

        # For each functionality_id, keep N true pairs and N false pairs where N is the
        # minimum number of occurences of functionality_id in "data_true" and "data_false". E.g., 
        # if for functionality_id=1 we fetched 30 true clones and 20 false clones, we will keep
        # min(20,30)=20 ture clones and 20 false clones so that the dataset is balanced
        data_pairs_all = pd.concat([
            data_true[data_true['functionality_id'] == func_id].head(
                min(
                    data_true['functionality_id'].value_counts().get(func_id, 0),
                    data_false['functionality_id'].value_counts().get(func_id, 0)
                )
            )
            for func_id in common_functionality_ids
        ] + [
            data_false[data_false['functionality_id'] == func_id].head(
                min(
                    data_true['functionality_id'].value_counts().get(func_id, 0),
                    data_false['functionality_id'].value_counts().get(func_id, 0)
                )
            )
            for func_id in common_functionality_ids
        ], ignore_index=True)

        # Keep only functionalities with >100 pairs
        for f,i in data_pairs_all.groupby('functionality_id').size().items():
            if i < 2*max_rows_per_functionality:
                data_pairs_all = data_pairs_all[data_pairs_all.functionality_id != f]

        data_pairs_all.to_pickle(data_pairs_all_fname)

    return data_pairs_all