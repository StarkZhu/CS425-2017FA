cd ../spark-2.2.0-bin-hadoop2.7;
./bin/spark-submit \
--class org.apache.spark.examples.graphx.$1 \
--master spark://fa17-cs425-g29-01.cs.illinois.edu:7077 \
--deploy-mode cluster \
--total-executor-cores $2 \
original-spark-examples_2.11-2.2.0.jar