/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.graphx

// $example on$
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "../../../MP4/graph_for_spark.txt")
    // Run PageRank
    val ranks = graph.staticPageRank(20).vertices
    // Print the result
    println(ranks.sortBy(_._2, ascending=false).take(25).mkString("\n"))
    // $example off$
    spark.stop()
  }
}
// scalastyle:on println
