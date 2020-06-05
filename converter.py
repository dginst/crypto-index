# %%
from datetime import datetime
import json
import pandas as pd
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.mongo_setup as mongo


# %%


# %%
df = pd.DataFrame.from_dict(data)

df

# %%

df["X"] = [int(datetime.strptime(x, "%d/%m/%Y").timestamp()) for x in df["X"]]

df


# %%
ciao = df.to_dict("records")

ciao

with open("result.json", "w") as fp:
    json.dump(ciao, fp)
