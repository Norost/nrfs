#!/usr/bin/env python3

import sys
from collections import defaultdict

def parse_trace(lines):
    for l in lines:
        if not l.strip():
            continue
        try:
            _, l = l.split('[nros:<', 1)
            id, l = l.split('>]', 1)
        except ValueError:
            #print('>>', l, end='')
            continue

        depth = 0
        for c in l:
            if c != ' ':
                break
            depth += 1
        assert depth % 2
        depth //= 2

        yield int(id), depth, [w.strip() for w in l.split(' ') if w]

def replay_total_mem(args):
    total = 0
    tasks = {}
    max_total = 0
    skip_next = False
    with open(args[0]) as f:
        for i, (id, depth, l) in enumerate(parse_trace(f.readlines())):
            if skip_next:
                assert l[0] == '-->'
                skip_next = False
            elif 'memory_reserve' == l[0]:
                assert id not in tasks, "multiple reserve"
                tasks[id] = depth, int(l[1]), i
            elif 'remove_memory' == l[0]:
                assert id not in tasks, "multiple remove"
                tasks[id] = depth, -int(l[1]), i
            elif 'force_grow' == l[0]:
                total += int(l[2])
                total -= int(l[1])
                max_total = max(max_total, total)
                print(f'<{id}> force_grow {int(l[2]) - int(l[1])} -> {total}')
                skip_next = True
            elif (t := tasks.get(id)) and l[0] == '-->':
                if l == ['-->', 'fail']:
                    print(f'<{id}> fail alloc {t[1]}')
                    continue
                if l == ['-->', 'not', 'finished']:
                    print(f'<{id}> unfinished {t[1]}')
                    continue
                if l[:2] == ['-->', 'id']:
                    continue

                del tasks[id]

                total += t[1]
                assert total == (t_total := int(l[1].split('/')[0])), \
                    f"total memory is off! Expected {total}, got {t_total}"
                max_total = max(max_total, total)

                if t[1] >= 0:
                    print(f'<{id}> alloc {t[1]} -> {total}')
                else:
                    print(f'<{id}> free  {-t[1]} -> {total}')

    print('Total:', total)
    print('Pending:')
    for id, t in sorted(tasks.items(), key = lambda kv: kv[1][2]):
        print(f'<{id}> {t[:2]}')
    print('Max reserved:', max_total)

def unfinished_tasks(args):
    tasks = set()
    with open(args[0]) as f:
        for id, depth, l in parse_trace(f.readlines()):
            if id not in tasks:
                tasks.add(id)
                print(f'start {id}')
                next_is_id = False
            elif l == ['-->', 'done']:
                print(f'stop {id}')
                assert depth == 0
                assert id != 0
                tasks.remove(id)
    if tasks:
        print(f'unfinished tasks: {", ".join(map(str, sorted(tasks)))}')
    else:
        print('no unfinished tasks')

def methods_coverage(args):
    cov = defaultdict(int)
    with open(args[0]) as f:
        next_is_id = False
        for id, depth, l in parse_trace(f.readlines()):
            if l[0] not in ('-->', '==>'):
                cov[l[0]] += 1
    for k, v in sorted(cov.items()):
        print(k, v)

def filter_task(args):
    f, filt_id = args
    with open(f) as f:
        prev_was_id = True
        for id, depth, l in parse_trace(f.readlines()):
            if id == int(filt_id):
                print('  ' * depth, *l)
                prev_was_id = True
            elif prev_was_id:
                print('...')
                prev_was_id = False

def replay_alloc(args):
    f, = args
    allocs = {}
    next_alloc = None
    with open(f) as f:
        for i, (id, depth, l) in enumerate(parse_trace(f.readlines())):
            if next_alloc is not None:
                assert l[0] == '-->'
                print(f'alloc {l[1]}+{next_alloc}')
                if l[1] == 'N/A':
                    break
                allocs[int(l[1])] = next_alloc
                next_alloc = None
            if 'alloc' == l[0]:
                next_alloc = int(l[1])
            elif 'free' == l[0]:
                lba, blocks = l[1].split('+')
                print(f'free  {lba}+{blocks}')
                assert allocs[int(lba)] == int(blocks)
                del allocs[int(lba)]
    print(allocs)

COMMANDS = {
    'replay_total_mem': replay_total_mem,
    'unfinished_tasks': unfinished_tasks,
    'methods_coverage': methods_coverage,
    'filter_task': filter_task,
    'replay_alloc': replay_alloc,
}

if len(sys.argv) < 2 or '--help' in sys.argv or '-h' in sys.argv:
    print(f'''usage: {sys.argv[0]} <tool>

Collection of tools to help interpret trace output.

tools: {", ".join(sorted(COMMANDS))}''')
    sys.exit(1)

COMMANDS[sys.argv[1]](sys.argv[2:])
