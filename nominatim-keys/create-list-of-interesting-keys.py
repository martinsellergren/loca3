import re


with open('osm2pgsql_output-gazetter.cpp_process_tags') as f:
    content = []
    started = False
    for line in f.readlines():
        if line == '***START***\n':
            started = True
        if line == '***STOP***\n':
            break
        if started:
            content.append(line)

keys = []
for line in content:
    m = re.search('strcmp\(k, "(.*)"\)', line)
    if m:
        keys.append(m.group(1))

keys.append('building')

keys = sorted(list(set(keys)))
for k in keys: print k
