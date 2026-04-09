from itertools import zip_longest

def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

n = 5000000

with open('Sales Trans3.txt') as f:
    for i, g in enumerate(grouper(n, f, fillvalue=''), 1):
        with open('sales_transaction{0}.txt'.format(i * n), 'w') as fout:
            fout.writelines(g)
