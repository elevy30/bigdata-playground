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
package spark.mllib.scala

import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.slf4j.{Logger, LoggerFactory}

object SampleDBSCANJob {

  val log: Logger = LoggerFactory.getLogger(SampleDBSCANJob.getClass)

  def main(args: Array[String]) {

//    if (args.length < 3) {
//      System.err.println("You must pass the arguments: <src file> <dest file> <parallelism>")
//      System.exit(1)
//    }

    args(0) = "_resources/data/dbscan/src.csv"
    args(1) = "_resources/data/dbscan/dst.csv"
    args(2) = "1"
    args(3) = "2"


    val (src, dest, maxPointsPerPartition, eps, minPoints) =  (args(0), args(1), args(2).toInt, args(3).toFloat, args(4).toInt)

    val destOut = dest.split('/').last

    val conf = new SparkConf().setAppName(s"DBSCAN(eps=$eps, min=$minPoints, max=$maxPointsPerPartition) -> $destOut")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.set("spark.storage.memoryFraction", "0.1")
    val sc = new SparkContext(conf)

    val data = sc.textFile(src)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    log.info(s"EPS: $eps minPoints: $minPoints")

    val model = DBSCAN.train(parsedData, eps, minPoints, maxPointsPerPartition)

    model.labeledPoints.map(p => s"${p.x},${p.y},${p.cluster}").saveAsTextFile(dest)
    log.info("Stopping Spark Context...")
    sc.stop()

  }
}
