# -*- coding: utf-8 -*-
import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List

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

with open("reddit_comments_dbAggrData_fromQuery20200604_135102.dat", mode='rb') as f:
  data = pickle.load(f)

print(f'before filter: {len(data)=}')

ignored = ["!",".",",",":","?",";","&gt"]
limit = 10
data = list(filter(lambda x: x[3]>limit and (x[0] not in ignored) and (x[1] not in ignored), data))
print(f'after filter: {len(data)=}')

df = optimize(pd.DataFrame.from_records(data, columns=['left','right','distance','occurrence']))
df = df[df['occurrence']>10].sort_values('occurrence', ascending=False)

#df['occurrence'] = df['occurrence'].apply(lambda x: x-(x%100))
notsame = df['left']!=df['right']
mean = df['occurrence'].mean()
notRare = df['occurrence']>mean
close = df['distance']==1
result = df[notsame & close]

result.insert(0, "pair", result['left']+" "+result['right'], True)

result = result.groupby('pair')['occurrence'].agg(['sum'])
#result = result.groupby('pair')['occurrence'].agg(pcs = ('occurrence','sum'))
result = result.sort_values('sum', ascending=False)[:20].rename(columns={'sum':'count'})
print(result)


result.plot.barh()
ax = plt.gca()
ax.set_ylim(ax.get_ylim()[::-1])
plt.tight_layout(pad=2, h_pad=1, w_pad=2)
plt.savefig('figure.pdf')
plt.show()
