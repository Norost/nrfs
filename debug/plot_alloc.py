import sys
import matplotlib.pyplot as plot
from collections import defaultdict

with open(sys.argv[1]) as f:
    data = f.read()

d = defaultdict(list)
for blk in data.split('\n\n'):
    for line in blk.split('\n'):
        if len((kv := line.split(':'))) == 2:
            d[kv[0]].append(int(kv[1]))

print(d.keys())

plot.plot([x >> 20 for x in d['active']])
plot.xlabel('sample #')
plot.ylabel('MiB')
plot.show()
