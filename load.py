import pandas as pd
import os

inputs = ['data/Books.csv', 'data/Movies_and_TV.csv']
outputs = ['output/book_df.csv', 'output/movie_df.csv']

chunk_size = 50000  # Adjust based on your memory

os.makedirs('data', exist_ok=True)
os.makedirs('output', exist_ok=True)

for input_path, output_path in zip(inputs, outputs):
    if input_path == 'data/Books.csv':
        continue
    
    if not os.path.exists(input_path):
        print(f"Skipping {input_path} as it does not exist.")
        continue

    print(f"Reading {input_path} in chunks...")
    
    # 1. Create a list to store processed chunks
    chunk_list = []

    # 2. Iterate through the file in chunks
    chunks = pd.read_csv(input_path, header=None, 
                        names=['item_id:token', 'user_id:token', 'rating:float', 'timestamp'],
                        chunksize=chunk_size)

    for i, chunk in enumerate(chunks):
        # 3. Apply transformations to each chunk
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s')

        # Convert ratings to binary (0 or 1)
        chunk.loc[chunk['rating:float'] < 4 , 'rating:float'] = 0
        chunk.loc[chunk['rating:float'] >= 4, 'rating:float'] = 1

        # Filter only ratings of 1 to reduce size
        chunk = chunk[chunk['rating:float'] == 1]

        # 4. Optional: Deduplicate within the chunk to save memory during concatenation
        chunk = chunk.sort_values(by='timestamp', ascending=False)
        chunk = chunk.drop_duplicates(subset=['user_id:token', 'item_id:token'], keep='first')

        chunk_list.append(chunk)
        
        if i % 10 == 0:
            print(f"  Processed chunk {i}...")

    # 5. Concatenate all chunks into a single DataFrame
    if not chunk_list:
        print(f"No valid data to process in {input_path}")
        continue
        
    df = pd.concat(chunk_list, ignore_index=True)

    # 6. Final Global Deduplication: Keep only the latest event for each user-item pair
    # Chaining these operations is often cleaner and avoids unnecessary intermediate variables
    df = (df.sort_values(by='timestamp', ascending=False)
            .drop_duplicates(subset=['user_id:token', 'item_id:token'], keep='first'))

    # Ensure columns are in the correct order
    df = df[['user_id:token', 'item_id:token', 'rating:float', 'timestamp']]

    # Save the result
    df.to_csv(output_path, index=False)
    print(f"Successfully processed and saved to {output_path}")
