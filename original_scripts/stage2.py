import pandas as pd

item_master1=pd.read_csv(r"clean.csv",usecols=["item_id","item_group_id","minishop_mother_item_code"])
item_master2=pd.read_csv(r"clean.csv",usecols=["item_id","item_group_id"])

item_master1=item_master1.fillna(0)
print(len(item_master1))

item_master2=item_master2.fillna(0)
item_master2["item_group_id"]=item_master2["item_group_id"].astype(int)
item_master1["item_group_id"]=item_master1["item_group_id"].astype(int)
item_master1["minishop_mother_item_code"]=item_master1["minishop_mother_item_code"].astype(int)
print(len(item_master2))
print("=====================================================================================")
new=pd.DataFrame()
temp_df = item_master1.loc[(item_master1['minishop_mother_item_code'] != 0) &
                           (item_master1["item_group_id"] == 0)]
print(len(temp_df))
temp_df.to_csv("left_table.csv")
print("=====================================================================================")

temp_df2=item_master2.loc[(item_master2["item_group_id"] != 0)]
print(len(temp_df2))
print("=====================================================================================")
temp_df2.to_csv("right_table.csv")
stage1 = pd.merge(temp_df,temp_df2,how="left",left_on="minishop_mother_item_code",right_on="item_id")
# stage1 = pd.merge(temp_df,temp_df2,how="outer",on="item_id")
# stage1=stage1.rename(columns={'item_id_x':"df_with_no_icasa"})
stage1.to_csv("xayz.csv")
print(stage1)
print("++++++++++++++++++++++++++++++++++")
pd.set_option('display.max_columns', None)

minishop_sorted=pd.merge(item_master1,stage1,left_on="item_group_id",right_on="item_id_x")
minishop_sorted.to_csv("stage1_sorted4.csv")
print(minishop_sorted)