import sys
import pandas as pd
import numpy as np
import pyarrow
import requests

print("Python:", sys.version.split()[0])
print("pandas:", pd.__version__)
print("numpy:", np.__version__)
print("pyarrow:", pyarrow.__version__)
print("requests:", requests.__version__)

df = pd.DataFrame({
    "store_id": ["S001", "S001", "S002"],
    "product": ["Milk 500ml", "Bread", "Milk 500ml"],
    "qty_on_shelf": [12, 0, 7],
})

df.to_parquet("data/sample.parquet", index=False)
back = pd.read_parquet("data/sample.parquet")
print(back)