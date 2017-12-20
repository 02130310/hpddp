#!/usr/bin/env python

import mincemeat
import os

datasource = []
for filename in os.listdir('sherlock'):
    with open('sherlock/'+filename, 'r') as f:
        datasource.append([filename, f.read()])
        text = f.read()
        datasource.append([filename, text])

datasource = dict(enumerate(datasource))
# looks like { 0: ["file1", "text1"], ... }

def mapfn(k, v):
    import nltk
    import string
    text = v[1]
    text = text.lower().translate(None, string.punctuation)
    tokens = nltk.word_tokenize(text)
    for token in tokens:
        yield (v[0], token), 1

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")

#convert to dict of dicts
def mapfn(k, v):
    yield k[1], { k[0]: v }

def reducefn(k, vs):
    d = {}
    for v in vs:
        d.update(v)
    return d

s = mincemeat.Server()
s.datasource = results
s.mapfn = mapfn
s.reducefn = reducefn
results2 = s.run_server(password="changeme")

import pandas as pd
df = pd.DataFrame(results2).T
df.fillna(0, inplace=True)
df.to_csv("sherlock-output.csv")
