$SPARK_HOME/bin/spark-submit \
--master spark://fa17-cs425-g29-01.cs.illinois.edu:7077 \
$SPARK_HOME/examples/src/main/python/pagerank.py /home/cs425/MP4/graph_for_spark.txt 10
