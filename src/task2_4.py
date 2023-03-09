import config as cfg
from task2_1 import dataframe

# Assuming by 'property with lowest price and highest rating', you mean the
# lowest priced room from all rooms with a rating equal to 100

# Filter out all rows with rating < 100, assign minimum price to variable,
# filter dataframe of rows whose price != minimum price, only keep accommodates
# column, write to csv
dataframe = dataframe.filter(dataframe.review_scores_rating == 100.)
min_price = dataframe.agg({'price': 'min'}).collect()[0]['min(price)']
dataframe.filter(dataframe.price == min_price)\
         .select('accommodates') \
         .write \
         .option("header", False) \
         .csv(str(cfg.dirs['out'] / 'out_2_4.txt'))
