import pandas as pd

part1=pd.read_csv("xayz.csv",index_col=0)
item_master=pd.read_csv(r"clean.csv",index_col=0)
part1=part1.drop(columns=["item_group_id_x","item_id_x"],axis=1)
part1["item_group_id_y"]=part1["item_group_id_y"].fillna(0)
part1["item_id_y"]=part1["item_id_y"].fillna(0)
part1["item_id_y"]=part1["item_id_y"].astype(int)
part1["item_group_id_y"]=part1["item_group_id_y"].astype(int)
part1=part1.rename(columns={"item_group_id_y":"new_item_id","item_id_y":"item_id"})
print(part1)
print("----------------------------------------------------------------------")
print(item_master)
newdf=pd.merge(item_master,part1,how="left",on="minishop_mother_item_code")
newdf.to_csv("stage1_done1.csv")


item_master=pd.read_csv("stage1_done1.csv",index_col=0)
pd.set_option('display.max_columns', None)

item_master["new_item_id"]=item_master['new_item_id'].fillna(0)
item_master["new_item_id"]=item_master['new_item_id'].astype(int)
item_master=item_master.fillna(0)
for i in range(len(item_master)):
    if (item_master.loc[i,"new_item_id"]==0):
        item_master.loc[i,"new_item_id"]= item_master.loc[i,"item_group_id"]

for i in range(len(item_master)):
    if (item_master.loc[i,"new_item_id"]==0):
        item_master.loc[i,"new_item_id"]=item_master.loc[i,"item_id_x"]

item_master=item_master.replace(0,"N/A")
item_master.to_csv("DONE.csv")
print(item_master.head(20))