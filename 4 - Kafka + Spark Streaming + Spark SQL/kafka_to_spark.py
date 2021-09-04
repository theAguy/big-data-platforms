### Stav Vaknin: 313581654
### Guy Assa: 204118616

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys


reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="Assigmnent4")
	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

	#Create Kafka Stream to Consume Data Comes From Twitter Topic
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
    
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    parsed.count().map(lambda x:'Number of Tweets in this batch: %s' % x).pprint()

    text_to_print = parsed.map(lambda tweet: tweet['text'])
    text_to_print.pprint(5)                                       # print the only 5 first tweets from the batch 
    text_to_print.filter(lambda x: 'USA' in x ).pprint()          # print tweets contain USA only (note that there no many tweets of COVID contain the word USA)
    text_to_print.filter(lambda x: 'US' in x ).pprint()           # Adding the word 'US' to the print 
    
    #Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()