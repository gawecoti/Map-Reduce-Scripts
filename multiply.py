__author__ = 'tgaweco'

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    #key: element i,j in AxB
    #value: element value
    row = record[1]
    column = record[2]
    value = record[3]

    if record[0] == 'a':
        for i in range(0, 5):
            mr.emit_intermediate((row, i), record)
    elif record[0] == 'b':
        for i in range(0, 5):
            mr.emit_intermediate((i, column), record)

def reducer(key, sequence):
    #key: none
    #value: value of each element in AxB
    a = [0 for i in range(5)]
    b = [0 for i in range(5)]
    i = 0
    j = 0
    k = 0

    while i < len(sequence):
        if sequence[i][0] == "a":
            index = sequence[i][2]
            a[index] = sequence[i][3]
        elif sequence[i][0] == "b":
            index = sequence[i][1]
            b[index] = sequence[i][3]
        i += 1

    sum = 0

    for e in range(0, 5):
        sum += a[e] * b[e]

    mr.emit((key[0], key[1], sum))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

