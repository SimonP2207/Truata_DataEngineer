import pyspark.sql.functions as sqfn

import config as cfg
from task2_1 import dataframe

# Multiple review score columns. Review ratings would be most obvious, but that
# scales up to 100. All other review scores types are specific. Assuming
# review_scores_value is the most relevant
dataframe.filter(dataframe.price > 5000)\
         .filter(dataframe.review_scores_value == 10.)\
         .select(sqfn.mean('bathrooms'), sqfn.mean('bedrooms'))\
         .withColumnRenamed('avg(bathrooms)', 'avg_bathrooms')\
         .withColumnRenamed('avg(bedrooms)', 'avg_bedrooms')\
         .write\
         .option("header", True)\
         .csv(str(cfg.dirs['out'] / 'out_2_3.txt'))
