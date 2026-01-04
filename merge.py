import dask.dataframe as dd
import os

inputs1 = ['output/movie_df.csv', 'output/book_df.csv']
inputs2 = ['data/Amazon-KG-5core-Movies_and_TV.inter', 'data/Amazon-KG-5core-Books.inter']
outputs = ['output/movie_interaction.csv', 'output/book_interaction.csv']

# Explicitly define dtypes for ID columns to ensure consistency and handle 'X' in ISBNs
dtypes = {
    'user_id:token': 'object',
    'item_id:token': 'object'
}

for input1, input2, output in zip(inputs1, inputs2, outputs):
    if not os.path.exists(input1) or not os.path.exists(input2):
        print(f"Skipping {output} because one of the inputs is missing.")
        continue

    print(f"Reading {input1} and {input2} with Dask...")
    
    # Read the processed interaction data (from load.py)
    df1 = dd.read_csv(input1, dtype=dtypes)
    
    # Read the KG interaction data (tab-separated)
    # Using blocksize=None if the .inter files are small or specialized, but usually dd.read_csv handles it.
    df2 = dd.read_csv(input2, sep='\t', dtype=dtypes)

    print(f"Merging data for {output}...")
    
    # Perform inner merge on user and item IDs
    # We only need the ID columns from df2 for the inner join
    df = dd.merge(df1, df2[['user_id:token', 'item_id:token']], 
                  on=['user_id:token', 'item_id:token'], 
                  how='inner')

    print(f"Computing and saving to {output}...")
    # single_file=True ensures one output file, mirrors the original Pandas behavior
    df.to_csv(output, index=False, single_file=True)
    
    print(f"Successfully created {output}\n")
