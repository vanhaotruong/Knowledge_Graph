import dask.dataframe as dd
import os

inputs = ['data/Books.csv', 'data/Movies_and_TV.csv']
outputs = ['output/book_df.csv', 'output/movie_df.csv']

os.makedirs('output', exist_ok=True)

# Explicitly define dtypes to prevent the 'X' in ISBNs from causing int errors
dtypes = {
    'item_id:token': 'object',  # Matches IDs like '030788726X'
    'user_id:token': 'object',
    'rating:float': 'float64',
    'timestamp': 'int64'
}

for input_path, output_path in zip(inputs, outputs):   
    if not os.path.exists(input_path):
        print(f"Skipping {input_path} as it does not exist.")
        continue

    print(f"Reading {input_path} with Dask...")
    
    # 1. Read CSV using Dask with explicit types
    df = dd.read_csv(input_path, header=None, 
                     names=['item_id:token', 'user_id:token', 'rating:float', 'timestamp'],
                     dtype=dtypes)

    # 2. Transformations
    # Convert timestamp (Dask handles this lazily)
    df['timestamp'] = dd.to_datetime(df['timestamp'], unit='s')

    # Filter only ratings >= 4 and treat them as 1.0 (Positive interaction)
    df = df[df['rating:float'] >= 4]
    df['rating:float'] = 1.0

    # 3. Deduplication: Get the latest row for each user-item pair
    # We group by user/item and take the max timestamp. 
    # This is MUCH faster in Dask than sort_values.
    print(f"Aggregating latest rows (deduplicating)...")
    df = df.groupby(['user_id:token', 'item_id:token']).agg({
        'timestamp': 'max',
        'rating:float': 'first' # Since they are all 1.0 now
    }).reset_index()

    # 4. Save the result
    # single_file=True mirrors your original logic of creating one output CSV
    print(f"Computing and saving to {output_path}...")
    df.to_csv(output_path, index=False, single_file=True)
    
    print(f"Successfully processed {input_path}\n")