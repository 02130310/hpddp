#!/usr/bin/env python

import csv
import mincemeat

datasource = []
with open('southpark/All-seasons.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        datasource.append([row[2], row[3]])

datasource = dict(enumerate(datasource))
# looks like { 0: ["character", "phrase"], ... }

def mapfn(k, v):
    yield v[0], v[1].decode('utf8')

def reducefn(k, vs):
    result = u" ".join(vs) # text
    import nltk
    result = nltk.word_tokenize(result) # individual words
    result = len(result) # number of unique words
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

import operator
with open('southpark-output.csv', 'wb') as f:
    fieldnames = ['character', 'unique words']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    data = [dict(zip(fieldnames, [k, v])) for k, v in sorted(results.items(),
                                    key=operator.itemgetter(1), reverse=True)]
    writer.writerows(data)
