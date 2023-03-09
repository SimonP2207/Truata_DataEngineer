import config as cfg
from task1_1 import rdd

# Split lines on commas, flatten, assign value of 1 per item, count values per
# unique item and sort in descending order
counts = rdd.map(lambda line: line.split(','))\
            .flatMap(lambda x: x)\
            .map(lambda x: (x, 1))\
            .reduceByKey(lambda a, b: a + b)\
            .sortBy(lambda x: x[1], ascending=False)\

if __name__ == '__main__':
    # Assign index to each line, filter indices >= 5 out, remove indices, write
    # to text file
    counts.zipWithIndex() \
          .filter(lambda x: x[1] < 5) \
          .map(lambda x: x[0])\
          .saveAsTextFile(str(cfg.dirs['out'] / 'out_1_3.txt'))
