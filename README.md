# Knowledge Graph Construction for Amazon Books 2018

This project builds a Knowledge Graph (KG) from the Amazon Books 2018 dataset.

## Prerequisites

- Python 3.x
- `pandas`, `tqdm`

## Setup

1.  **Download the Data**:
    Go to the [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html) page.
    Download the following files for the "Books" category:
    - `reviews_Books_5.json.gz` (5-core subset is recommended for cleaner data)
    - `meta_Books.json.gz` (Metadata)

2.  **Organize Files**:
    Create a `data` folder in this directory and place the downloaded files inside.
    ```
    Knowledge_Graph/
    ├── build_kg.py
    ├── data/
    │   ├── reviews_Books_5.json.gz
    │   └── meta_Books.json.gz
    └── README.md
    ```

## Usage

Run the script to build the graph:
```bash
python build_kg.py
```

## Output

The script will create an `output` directory containing:
- `kg_final.txt`: The knowledge graph triples (Head, Relation, Tail).
- `entity_map.txt`: Mapping of entity strings (IDs) to integers.
- `relation_map.txt`: Mapping of relation types to integers.
