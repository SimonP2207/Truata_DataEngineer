import pyspark.sql.functions as sqfn

import config as cfg
from task2_1 import dataframe

# Get minimum-price/maximum-price/total-count, rename columns according to task
# definition, write to csv with header
dataframe.select(sqfn.min('price'), sqfn.max('price'), sqfn.count('price'))\
         .withColumnRenamed('min(price)', 'min_price')\
         .withColumnRenamed('max(price)', 'max_price')\
         .withColumnRenamed('count(price)', 'count')\
         .write\
         .option("header", True)\
         .csv(str(cfg.dirs['out'] / 'out_2_2.txt'))
