#!/usr/bin/env python

import mincemeat
import csv

arrs = { }

with open('matrices.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    csvreader.next() # skip the header
    for row in csvreader:
        # { (name, i, j): value }
        arrs[(row[0], int(row[1]), int(row[2]))] = float(row[3])

# find sizes of the matrices
n = max(arrs, key=lambda x: x[1])[1]
n = max(n, max(arrs, key=lambda x: x[2])[2])
n = n + 1

# append it to our data for workers to use
for k, v in arrs.iteritems():
    arrs[k] = (v, n)

# { (name, i, j): (value, n) }
def mapfn(key, v):
    value = float(v[0])
    n = v[1]
    if (key[0] == 'a'):
        for k in range(0, n):
            # a: all columns
            yield (key[1], k), (key[0], key[2], value)
    else:
        for i in range(0, n):
            # b: all rows
            yield (i, key[2]), (key[0], key[1], value)

# (i, k) [(name, j, value)]
def reducefn(k, vs):
    a = list(filter(lambda x: x[0] == 'a', vs)) # filter by name
    b = list(filter(lambda x: x[0] == 'b', vs))
    a.sort(lambda x, y: cmp(x[1], y[1])) # sort by j
    b.sort(lambda x, y: cmp(x[1], y[1]))
    zipped = zip(a, b) # zip A's row i and B's col k
    s = sum((x[0][2] * x[1][2] % 97) for x in zipped) % 97
    return s

s = mincemeat.Server()
s.datasource = arrs
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")

with open('matrices-output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['#matrix', 'i', 'j', 'value'])
    import operator
    for key, value in sorted(results.iteritems(), key=operator.itemgetter(0)):
        writer.writerow(('c', key[0], key[1], value))
