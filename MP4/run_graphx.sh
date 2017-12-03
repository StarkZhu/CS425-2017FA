cd ../spark-2.2.0-bin-hadoop2.7;
$SPARK_HOME/bin/spark-submit \
--class org.apache.spark.examples.graphx.SSSPExample \
--master spark://fa17-cs425-g29-01.cs.illinois.edu:7077 \
--deploy-mode cluster \
original-spark-examples_2.11-2.2.0.jar