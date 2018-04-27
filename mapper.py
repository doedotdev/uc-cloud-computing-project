#!/usr/bin/env python

# Benjamin Horn - Mapper.py

import sys

for line in sys.stdin:
    parse = line.strip().upper().split(',')
    i = 0
    print(len(parse))
    if i < 5 and len(parse) == 8:
        # Station ID
        print(parse[0])
        # YMD
        print(parse[1])
        # element
        print(parse[2])
        # element value
        print(parse[3])
        # measurement flag
        print(parse[4])
        # Q - FLAG
        print(parse[5])
        # S - FLAG
        print(parse[6])
        # OBS - TIME
        print(parse[7])
        i += 1
        print('%s,%s,%s,%s,' % (parse[1], parse[0], parse[2], parse[3]))
