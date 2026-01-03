import json
import gzip
import pandas as pd
import os
from collections import defaultdict
from tqdm import tqdm

# Configuration
DATA_DIR = './data'
REVIEWS_FILE = 'reviews_Books_5.json.gz'  # Replace with your actual file name
META_FILE = 'meta_Books.json.gz'          # Replace with your actual file name
OUTPUT_DIR = './output'

def parse(path):
    print(f"Parsing {path}...")
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

def get_dataframe(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
        # For demonstration memory limits, break early if needed
        # if i > 100000: break 
    return pd.DataFrame.from_dict(df, orient='index')

def build_knowledge_graph():
    # Ensure directories exist
    if not os.path.exists(DATA_DIR):
        print(f"Directory '{DATA_DIR}' not found. Please create it and place dataset files there.")
        return
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 1. Load Data
    reviews_path = os.path.join(DATA_DIR, REVIEWS_FILE)
    meta_path = os.path.join(DATA_DIR, META_FILE)

    if not os.path.exists(reviews_path) or not os.path.exists(meta_path):
        print(f"Files not found in {DATA_DIR}. Please download Amazon Books 2018 dataset.")
        return

    print("Loading Reviews...")
    # Using a generator to avoid loading everything into memory at once if possible, 
    # but for simplicity in this script we'll load into pandas/dicts
    
    # Mappings
    entity_to_id = {}
    relation_to_id = {
        'interact': 0,
        'also_bought': 1,
        'also_viewed': 2,
        'belongs_to_category': 3,
        'brand_is': 4
    }
    
    triples = []

    def get_id(entity, type_prefix):
        key = f"{type_prefix}_{entity}"
        if key not in entity_to_id:
            entity_to_id[key] = len(entity_to_id)
        return entity_to_id[key]

    # 2. Process Reviews (User-Item Interactions)
    # We treat these as interactions or "RATED"
    # Format: reviewerID, asin, overall, ...
    print("Processing Interactions...")
    count = 0
    for d in parse(reviews_path):
        user_id = get_id(d['reviewerID'], 'user')
        item_id = get_id(d['asin'], 'item')
        
        # Add Interaction Triple
        triples.append((user_id, relation_to_id['interact'], item_id))
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count} reviews...")

    print(f"Total Interactions: {len(triples)}")

    # 3. Process Metadata (Item attributes)
    # Format: asin, title, categories, brand, also_buy, also_view...
    print("Processing Metadata...")
    count = 0
    for d in parse(meta_path):
        asin = d.get('asin')
        if not asin: continue
        
        item_id = get_id(asin, 'item')

        # Also Bought
        also_bought = d.get('also_buy', [])
        for related_asin in also_bought:
            related_id = get_id(related_asin, 'item')
            triples.append((item_id, relation_to_id['also_bought'], related_id))

        # Also Viewed
        also_viewed = d.get('also_view', [])
        for related_asin in also_viewed:
            related_id = get_id(related_asin, 'item')
            triples.append((item_id, relation_to_id['also_viewed'], related_id))

        # Categories
        # formats like [['Books', 'Literature', '...']]
        categories = d.get('category', [])
        for cat_list in categories:
            for cat in cat_list:
                cat_id = get_id(cat, 'category')
                triples.append((item_id, relation_to_id['belongs_to_category'], cat_id))
        
        # Brand
        brand = d.get('brand')
        if brand:
            brand_id = get_id(brand, 'brand')
            triples.append((item_id, relation_to_id['brand_is'], brand_id))
            
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count} metadata entries...")

    # 4. Save Results
    print("Saving KG...")
    
    # Save Triples
    df_triples = pd.DataFrame(triples, columns=['head_id', 'relation_id', 'tail_id'])
    df_triples.to_csv(os.path.join(OUTPUT_DIR, 'kg_final.txt'), sep='\t', index=False, header=False)
    
    # Save Entity Map
    with open(os.path.join(OUTPUT_DIR, 'entity_map.txt'), 'w', encoding='utf-8') as f:
        for entity_str, eid in entity_to_id.items():
            f.write(f"{entity_str}\t{eid}\n")
            
    # Save Relation Map
    with open(os.path.join(OUTPUT_DIR, 'relation_map.txt'), 'w', encoding='utf-8') as f:
        for rel_str, rid in relation_to_id.items():
            f.write(f"{rel_str}\t{rid}\n")

    print("Done! Knowledge Graph construction complete.")
    print(f"Total Entities: {len(entity_to_id)}")
    print(f"Total Triples: {len(triples)}")

if __name__ == "__main__":
    build_knowledge_graph()
