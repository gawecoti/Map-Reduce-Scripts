__author__ = 'tgameco'

import MapReduce
import sys

mr = MapReduce.MapReduce()

# The input are key-value pairs of two friends. The output are tuples of 
# asymmetric friendships (that is, one person follows a person but isn't followed back by that same person).

def mapper(record):
    #key: person_a, person_b
    #value: 1
    person_a = record[0]
    person_b = record[1]

    mr.emit_intermediate((person_a, person_b), 1)
    mr.emit_intermediate((person_b, person_a), 1)

def reducer(key, list_of_friends):
    #key: person
    #value: n/a
    if len(list_of_friends) < 2:
        mr.emit((key))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

