__author__ = 'tgaweco'

import MapReduce
import sys

mr = MapReduce.MapReduce()

# The input are key-value pairs of document id's and accompanying text. 
# The output are key-value pairs of words and the documents they're found in

def mapper(record):
    #key: word
    #value: document_id
    document_id = record[0]
    txt = record[1]
    t = txt.split()
    
    for word in t:
        mr.emit_intermediate(word, document_id)

def reducer(key, list_of_document_ids):
    #key: words
    #value: list of document ids associated with the word
    list_of_ids = list()
    
    for d in list_of_document_ids:
        list_of_ids.append(d)
    mr.emit((key,list(set(list_of_ids))))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

