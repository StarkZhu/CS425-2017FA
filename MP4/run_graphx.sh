$SPARK_HOME/bin/spark-submit \
--class org.apache.spark.examples.SparkPageRank \
--master spark://fa17-cs425-g29-01.cs.illinois.edu:7077 \
--deploy-mode cluster \
$SPARK_HOME/examples/jars/spark-examples_2.11-2.2.0.jar /home/cs425/MP4/com-amazon.ungraph.txt 1
