import pandas as pd

item_master=pd.read_csv("DONE.csv",usecols=[],index_col=0)
pd.set_option('display.max_columns', None)
item_master["new_item_id"]=item_master["new_item_id"].astype(str)
item_master["item_name"]=item_master["item_name"].astype(str)
item_master = item_master.groupby("new_item_id", as_index=False).agg(lambda x: x.tolist())
a=dict(zip(item_master.new_item_id,item_master.item_name))
for key, values in a.items():
    str1 = ""
    if len(values) > 1:
        for i in values:
            str1 += i + "//"
        str1 = str1[:-2]
        k = {key: str1}
        a.update(k)
    else:
        str1 = values[0]
        k = {key: str1}
        a.update(k)
r = pd.DataFrame([a])
r = r.swapaxes("index","columns")
r["new_item_id"]=r.index
r.reset_index()
print(r)
print(len(r))
print(len(item_master))
hello_df=pd.merge(r,item_master,on="new_item_id")
print(hello_df.columns)
hello_df=hello_df.rename(columns={0:"item_name","item_group_id":"item_id","item_id_x":"legacy_id","item_name":"item_list"})
hello_df=hello_df.drop(columns=["item_list"],axis=1)
# print(hello_df)
hello_df.to_csv("merged_items.csv")
hello=hello_df.explode("legacy_id")
print(hello)
hello.to_csv("explode.csv")

print(item_master)