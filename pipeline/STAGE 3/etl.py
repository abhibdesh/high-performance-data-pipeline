import datetime
import pandas as pd
import dask.dataframe as dd

def etl(ddf):
    ddf["country"] = "IT"
    ddf["sales_line"] = "SALES_LINE"
    ddf["check_updated"] = True
    ddf["margin"] = 0.0

    ddf = ddf.rename(columns={
        "Quantity": "units",
        "Product Number": "item",
        "Store name": "store",
        "Date": "transaction_date",
        "Sales Value": "sales"
    })

    ddf["item"] = ddf["item"].astype(str)
    ddf["units"] = ddf["units"].astype(float)
    ddf["transaction_date"] = dd.to_datetime(ddf["transaction_date"])

    ddf["week_ending_date"] = ddf["transaction_date"] + pd.offsets.Week(weekday=6)
    ddf["date_folder"] = datetime.date.today()

    return ddf