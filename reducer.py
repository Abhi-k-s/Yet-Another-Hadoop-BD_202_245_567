#!/usr/bin/env python3
import sys
dictionary = {}
for line in sys.stdin:     
    dictionary[int(line)] = dictionary.setdefault(int(line), 0) + 1    

for i in sorted(dictionary.keys()):
    print(i, dictionary[i])
