import config as cfg
from task1_1 import rdd

# Split lines on commas, flatten, get unique values
unique_products = rdd.map(lambda line: line.split(','))\
                     .flatMap(lambda x: x)\
                     .distinct()

if __name__ == '__main__':
    unique_products.saveAsTextFile(str(cfg.dirs['out'] / 'out_1_2a.txt'))

    with open(str(cfg.dirs['out'] / 'out_1_2b.txt'), 'wt') as f:
        f.write(f'Count:\n{unique_products.count()}')
