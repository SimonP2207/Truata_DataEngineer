from pyspark import SparkConf, SparkContext
import config as cfg

# Read in groceries.csv to rdd
conf = SparkConf().setAppName('task1').setMaster('local')
sc = SparkContext(conf=conf)
rdd = sc.textFile(str(cfg.files['groceries']))
