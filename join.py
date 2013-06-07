__author__ = 'tgaweco'


import MapReduce
import sys

mr = MapReduce.MapReduce()

# Performs a SQL join using Map Reduce

def mapper(record):
    #key: order_id
    #value: record
    order_id = record[1]
    value = record
    mr.emit_intermediate(order_id, value)

def reducer(key, list_of_document_ids):
    #key: order_id
    #value: joined documents
    join = list(list_of_document_ids[0])
    
    for row in list_of_document_ids:
        if (join[0] != row[0]):
            out = join + row
            mr.emit(out)



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

