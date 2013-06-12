Map Reduce Scripts in Python
===========

This repository contains several scripts in Python that apply the Map Reduce programming model. 

1. inverted_index.py - The input are key-value pairs of document id's and accompanying text. The output are key-value pairs of words and the documents they're found in.
2. join.py - Performs a SQL join using Map Reduce.
3. friend_count.py -  The input are key-value pairs of two friends. The output are key-value pairs of each person and their number of friends.
4. asymmetric_friendship.py - The input are key-value pairs of two friends. The output are tuples of asymmetric friendships (that is, one person follows a person but isn't followed back by that same person).
5. unique_trims.py - The input are key-value pairs of ids and sequence of nucleotides. The output are trimmed sequences of nucleotides with the last 10 base pairs removed.
6. multiply.py - Multiplies two 5x5 matrices

To Run
=========

```
python inverted_index.py data/records.json
```

```
Use the correct .json file for each of the six scripts.
```
