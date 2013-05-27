__author__ = 'tgameco'

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    #key: sequence_id
    #value: nucleotides
    sequence_id = record[0]
    nucleotides = record[1]
    mr.emit_intermediate(nucleotides[:len(nucleotides)-10],1)

def reducer(key, dna_sequence):
    #key: sequence_id
    #value: trimmed nucleotide
    mr.emit(key)

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

