from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import collect_list
import numpy
import os
import sys
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("OurCodeIsReal").getOrCreate()
path="hdfs://master:9000/user/user/input_ex2/*.txt"


df=spark.read.text(path)
df=df.withColumn("filename",input_file_name())
setFileName=udf(os.path.basename,StringType())
df=df.withColumn("filename",setFileName("filename"))        
df_grouped=df.groupBy("filename").agg(collect_list("value").alias("text"))
df_grouped = df_grouped.withColumn("text", concat_ws(" ", "text"))
df_grouped=df_grouped.withColumn("text",regexp_replace("text","\\s+"," "))
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenized=tokenizer.transform(df_grouped)
tokenized=tokenized.drop("text")
df_grouped.unpersist()
cv = CountVectorizer(inputCol="words", outputCol="features", binary=True)
model=cv.fit(tokenized)
vectorized=model.transform(tokenized)
vectorized=vectorized.drop("words")
tokenized.unpersist()
mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=int(sys.argv[1]))
lshmodel=mh.fit(vectorized)
vectorized=lshmodel.transform(vectorized)
threshold = 0.8
joined=lshmodel.approxSimilarityJoin(vectorized,vectorized,threshold)
joined_sim=joined.withColumn("distCol",1-col("distCol")).withColumnRenamed('distCol','similarity')
joined_sim=joined_sim.filter(col("datasetA.filename")!=col("datasetB.filename"))
joined_sim=joined_sim.filter((~col("datasetA.filename").contains("paraphrase"))&(~col("datasetB.filename").contains("original")))
output_path="hdfs://master:9000/user/user/output_ex2/"+sys.argv[2]
df_selected=joined_sim.select(joined_sim["datasetA.filename"].alias("filename1"),joined_sim["datasetB.filename"],col("similarity").cast("string").alias("similarity_str"))
df_selected=df_selected.orderBy(-col("similarity_str").cast("double")).limit(10)
df_selected.write.csv(output_path,header=True,mode="overwrite")
spark.stop()

