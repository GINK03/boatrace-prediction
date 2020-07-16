import json
import glob
import pandas as pd


objs = []
for filename in glob.glob("var/work_cache/*"):
    objs += json.load(open(filename))

df = pd.DataFrame(objs)
print(df)

df.to_csv("var/collect_data.csv.orig", index=None)
