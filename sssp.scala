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
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example use the Pregel operator to express computation
 * such as single source shortest path
 * Run with
 * {{{
 * bin/run-example graphx.SSSPExample
 * }}}
 */
object SSSP {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // A graph with edge attributes containing distances
    val graph = GraphLoader.edgeListFile(sc, "../../../MP4/graph_for_spark.txt")
    // val graph: Graph[Long, Double] =
    //  GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 1 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val result = ShortestPaths.run(graph, Seq(sourceId))
    println(result.vertices.sortBy(_._2.get(sourceId), ascending=false).take(25).mkString("\n"))
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
