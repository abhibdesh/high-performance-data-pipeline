import pandas as pd

def load_csv(path):
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} records")
    return df