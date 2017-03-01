import os
import sys
import hdfs
import conda
from email.parser import Parser
from textblob import TextBlob

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql.types import *
    from pyspark.sql import SQLContext
    print('Successfully import Spark Mudules')
except ImportError as e:
    print('Cannot import Spark Modules', e)
    sys.exit(1)

conf = SparkConf().setAppName('enron-sentiment').setMaster('yarn')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

sqlContext.setConf("spark.sql.shuffle.partitions", u"8")
emails = []
email_dir = 'data/enron_raw/small/lay-k/inbox'
sentiments = []
    
parsedEmails = sc.wholeTextFiles(email_dir).map(lambda x: x[1].encode('utf-8', 'ignore')).map(lambda x: Parser().parsestr(x))
raw_sentiments = parsedEmails.map(lambda x: (x['from'],TextBlob(x.get_payload()).sentiment.polarity))
grouped_sentiments = raw_sentiments.aggregateByKey((0,0), 
                                      lambda a,b: (a[0] + b,    a[1] + 1),
                                      lambda a,b: (a[0] + b[0], a[1] + b[1])).map(lambda x: (x[0], x[1][0]/x[1][1]))

fields = [StructField('Employee', StringType(), False), StructField('Sentiment', FloatType(), True)]
schema = StructType(fields)

sentDF = sqlContext.createDataFrame(grouped_sentiments, schema).sort('Sentiment', ascending=False)
sentDF.write.mode('Overwrite').save('enron_sentiment/analysis/raw_sentiments/parquet', format='parquet')
sentDF.write.format('com.databricks.spark.csv').mode('Overwrite').save('enron_sentiment/analysis/raw_sentiments/text')










#rdd1.aggregateByKey((0,0), lambda a,b: (a[0] + b,    a[1] + 1),
#                                      lambda a,b: (a[0] + b[0], a[1] + b[1]))
#.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \

#parsedEmails.foreach(lambda x: sentiments.append([x['from'], x.get_payload()]))


#fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
#schema = StructType(fields)



#URI           = sc._gateway.jvm.java.net.URI
#Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
#FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
#Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
#fs = FileSystem.get(URI('hdfs://tomesd-510cdsw-1.vpc.cloudera.com:8020'), Configuration())
#status = fs.listStatus(Path(email_dir))
#for fileStatus in status:
#    emails.append(str(fileStatus.getPath()))
