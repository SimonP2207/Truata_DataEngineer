from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
import config as cfg
from task3_1 import pandas_dataframe

spark = SparkSession.builder \
                    .master("local") \
                    .appName("task_3_2") \
                    .getOrCreate()

# Import Iris data from previously defined pandas DataFrame from task3_1
spark_dataframe = spark.createDataFrame(pandas_dataframe)
feature_cols = spark_dataframe.columns[:-1]
assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
data = assembler.transform(spark_dataframe)

# Convert 'class' from str to int and define as label using StringIndexer class
data = data.select(['features', 'class'])
label_indexer = StringIndexer(inputCol='class', outputCol='label').fit(data)
mapping = dict(enumerate(label_indexer.labels))  # to recover class name later
data = label_indexer.transform(data)

# Pred data, should be ['Iris-setosa'] and ['Iris-virginica']
pred_data = spark.createDataFrame(
    [(5.1, 3.5, 1.4, 0.2), (6.2, 3.4, 5.4, 2.3)],
    ["sepal_length", "sepal_width", "petal_length", "petal_width"]
)
pred_data = assembler.transform(pred_data)
pred_data = pred_data.select(['features', ])

logreg = LogisticRegression()
model = logreg.fit(data.select(['features', 'label']))
prediction = model.transform(pred_data).select('prediction')
function = udf(lambda col1 : mapping[col1], StringType())

# Recover class name from index assigned by label_indexer, keep class column
# only, write to csv
prediction.withColumn('class', function(col('prediction')))\
          .select('class')\
          .write\
          .option("header", True)\
          .csv(str(cfg.dirs['out'] / 'out_3_2.txt'))
