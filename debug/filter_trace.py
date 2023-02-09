import sys

with open(sys.argv[1]) as f:
    for l in f.readlines():
        if any(w in l for w in sys.argv[2:]):
            print(l.strip())

