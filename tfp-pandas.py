# -*- coding: utf-8 -*-
import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List
import inspect

def optimize_floats(df: pd.DataFrame) -> pd.DataFrame:
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
    return df


def optimize_ints(df: pd.DataFrame) -> pd.DataFrame:
    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
    return df


def optimize_objects(df: pd.DataFrame, datetime_features: List[str]) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object']):
        if col not in datetime_features:
            num_unique_values = len(df[col].unique())
            num_total_values = len(df[col])
            if float(num_unique_values) / num_total_values < 0.5:
                print(col)
                df[col] = df[col].astype('category')
        else:
            df[col] = pd.to_datetime(df[col])
    return df



def optimize(df: pd.DataFrame, datetime_features: List[str] = []):
    return df
    #return optimize_floats(optimize_ints(optimize_objects(df, datetime_features)))
    return optimize_floats(optimize_ints(df))

data = []

with open("reddit_comments_dbAggrData_fromQuery20200604_165429.dat", mode='rb') as f:
  data = pickle.load(f)

print(f'before filter: {len(data)=}')

ignored = ["!",".",",",":","?",";","&gt","//www"]
limit = 10
data = list(filter(lambda x: x[3]>limit and (x[0] not in ignored) and (x[1] not in ignored), data))
print(f'after filter: {len(data)=}')

df = optimize(pd.DataFrame.from_records(data, columns=['left','right','distance','occurrence']))
df = df[df['occurrence']>10].sort_values('occurrence', ascending=False)
df['left'] = df['left'].apply(lambda x: x.strip())
df['right'] = df['right'].apply(lambda x: x.strip())

#df['occurrence'] = df['occurrence'].apply(lambda x: x-(x%100))
notsame = df['left']!=df['right']
mean = df['occurrence'].mean()
notRare = df['occurrence']>mean
close = df['distance']==1
result = df[notsame & close]

result.insert(0, "pair", result['left']+" "+result['right'], True)
print(inspect.currentframe().f_lineno, "resulthead[pair]:", result['pair'].head())

#print(f'before group: {len(result.index)=}')
result = result.groupby('pair')['occurrence'].agg(['sum']).rename(columns={'sum':'count'})

#print(f'after group: {len(result.index)=}')
result_top = result.sort_values('count', ascending=False)[:50]
#print(f'after _top: {len(result.index)=}')

print(result_top)


result_top.plot.barh(figsize=(5,7))
ax = plt.gca()
ax.set_ylim(ax.get_ylim()[::-1])
plt.tight_layout(pad=2, h_pad=1, w_pad=2)


result_bottom = result.sort_values('count', ascending=True)[:50]
result_bottom.plot.barh(figsize=(12,7))
plt.savefig('figure.pdf')

#plt.show()

with open("pandas_onedistance_out.dat", mode="wb") as outf:
    pickle.dump(result, outf)

topmost = result_top.iloc[0]['count']
result.insert(len(result.columns), "rel_pct", 100*result['count']/topmost)

occ_classes = []

for i in range(20,101):
    print(i)
    percentile = result[(result['rel_pct']<i+1) & (result['rel_pct']>=i)].reset_index()

    if not percentile.empty:
        #print("============================")
        #print(percentile.head())

        #print(percentile['pair'])
        lst = percentile['pair'].to_list()
        record = ", ".join(lst)
        print(i, record)
        occ_classes.append([i, record])

print("==============================")
print(occ_classes)
occ_cl_df = pd.DataFrame.from_records(occ_classes, columns=["percentile","pairs"])
print(occ_cl_df)

plt.close("all")

occ_cl_df.plot.barh(x='pairs', y='percentile', figsize=(11.69,8.27))
plt.tight_layout(pad=2, h_pad=1, w_pad=1)
plt.subplots_adjust(left=0.6)
# fig = occ_cl_df.plot.pie(labels=occ_cl_df['pairs'], y='percentile', explode=[0.1]*(len(occ_cl_df.index)-1)+[0.2])
ax = plt.gca()
plt.grid(True, axis='x')
# ax.legend_.remove()
# ax.set_ylim(ax.get_ylim()[::-1])
# plt.tight_layout(pad=2, h_pad=1, w_pad=2)
# plt.show()
plt.savefig('popular.pdf')
