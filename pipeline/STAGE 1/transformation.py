import pandas as pd

def clean_data(df):
    df = df.drop_duplicates()
    df = df.fillna("N/A")
    
    # add your column transformations here
    df["country"] = "HU"
    
    return df