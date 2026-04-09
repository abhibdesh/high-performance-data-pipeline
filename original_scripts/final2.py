import pandas as pd

df=pd.read_csv("final_sales.csv",index_col=0)
print(len(df))
df=df.drop_duplicates(subset=["store","item","transaction_date",'sales','units','week_ending_date','legacy_item_id'],keep="first")
print(len(df))