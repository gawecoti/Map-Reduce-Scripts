__author__ = 'tgaweco'

import MapReduce
import sys

mr = MapReduce.MapReduce()

# The input are key-value pairs of two friends. 
# The output are key-value pairs of each person and their number of friends.

def mapper(record):
    #key: person_a
    #value: person_b
    person_a = record[0]
    person_b = record[1]
    mr.emit_intermediate(person_a, person_b)

def reducer(key, list_of_friends):
    #key: person
    #value: friends
    num_friends = 0
    for f in list_of_friends:
        num_friends += 1
    mr.emit((key, num_friends))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

