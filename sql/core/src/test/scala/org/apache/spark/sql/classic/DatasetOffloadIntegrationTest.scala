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

package org.apache.spark.sql.classic

import java.util.UUID

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.s3offload.S3Utils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl

case class Person(name: String, age: Int)

class DatasetOffloadIntegrationTest extends SparkFunSuite {

  def withSparkSession(bucket: String, prefix: String, extraConf: Map[String, String] = Map())
                      (testCode: SparkSession => Unit): Unit = {
    val baseConf = Map(
      "spark.s3.offload.enabled" -> "true",
      "spark.s3.bucket" -> bucket,
      "spark.s3.prefix" -> prefix
    ) ++ extraConf

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Dataset S3 Offload Test")

    baseConf.foreach { case (k, v) => conf.set(k, v) }

    val spark = SparkSession.builder().config(conf).getOrCreate()

    try {
      testCode(spark)
    } finally {
      S3Utils.deletePrefix(bucket, prefix)
      spark.stop()
    }
  }

  test("Dataset[Row] offloads and collects correctly") {
    val bucket = "spark-bucket"
    val prefix = s"test-ds-row-${UUID.randomUUID()}"

    withSparkSession(bucket, prefix) { spark =>
      import spark.implicits._

      val df = Seq(
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35)
      ).toDF("name", "age")

      val result: Array[Row] = df.collectToS3()

      assert(result.map(_.getString(0)).toSet == Set("Alice", "Bob", "Charlie"))
    }
  }

  test("Typed Dataset[Person] offloads and collects correctly") {
    val bucket = "spark-bucket"
    val prefix = s"test-ds-typed-${UUID.randomUUID()}"

    withSparkSession(bucket, prefix) { spark =>
      import spark.implicits._

      val ds: Dataset[Person] = Seq(
        Person("Dan", 40),
        Person("Eve", 45)
      ).toDS()

      val result: Array[Person] = ds.collectToS3()

      assert(result.contains(Person("Dan", 40)))
      assert(result.contains(Person("Eve", 45)))
    }
  }

  test("Dataset with compression enabled collects correctly") {
    val bucket = "spark-bucket"
    val prefix = s"test-ds-compress-${UUID.randomUUID()}"

    val extraConf = Map("spark.s3.offload.compress" -> "true")

    withSparkSession(bucket, prefix, extraConf) { spark =>
      import spark.implicits._

      val ds = (1 to 100).map(n => Person(s"Person$n", 20 + (n % 10))).toDS()
      val result: Array[Person] = ds.collectToS3()

      assert(result.length == 100)
    }
  }
}
