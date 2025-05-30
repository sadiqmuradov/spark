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

package org.apache.spark.s3offload

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class S3OffloadIntegrationTest extends SparkFunSuite {

  def withSparkContext(bucket: String, prefix: String, extraConf: Map[String, String] = Map())
                      (testCode: SparkContext => Unit): Unit = {
    val baseConf = Map(
      "spark.s3.offload.enabled" -> "true",
      "spark.s3.bucket" -> bucket,
      "spark.s3.prefix" -> prefix
    ) ++ extraConf

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("S3 Offload Integration Test")

    baseConf.foreach { case (k, v) => conf.set(k, v) }

    val sc = new SparkContext(conf)
    try {
      testCode(sc)
    } finally {
      S3Utils.deletePrefix(bucket, prefix)
      sc.stop()
    }
  }

  test("Basic S3 offload and collect works") {
    val bucket = "spark-bucket"
    val prefix = s"test-basic-${UUID.randomUUID()}"

    withSparkContext(bucket, prefix) { sc =>
      val data = (1 to 100)
      val rdd = sc.parallelize(data, 4)
      val collected = rdd.collectToS3()
      assert(collected.sorted sameElements data)
    }
  }

  test("S3 offload with compression enabled") {
    val bucket = "spark-bucket"
    val prefix = s"test-compress-${UUID.randomUUID()}"

    val conf = Map(
      "spark.s3.offload.compress" -> "true"
    )

    withSparkContext(bucket, prefix, conf) { sc =>
      val rdd = sc.parallelize(1 to 50, 2)
      val collected = rdd.collectToS3()
      assert(collected.sorted sameElements (1 to 50))
    }
  }

  test("Retry logic should tolerate transient failure (mock test)") {
    // NOTE: This test is a placeholder. To simulate real S3 retry,
    // you would need a proxy/mocked S3 service or a library like LocalStack
    // with fault injection, which is out of scope for unit-level test.

    // Here we just verify the retry config doesn't break flow
    val bucket = "spark-bucket"
    val prefix = s"test-retry-${UUID.randomUUID()}"

    val conf = Map(
      "spark.s3.offload.retries" -> "5",
      "spark.s3.offload.backoff.ms" -> "50"
    )

    withSparkContext(bucket, prefix, conf) { sc =>
      val rdd = sc.parallelize(Seq("retry", "logic", "test"), 1)
      val collected = rdd.collectToS3()
      assert(collected.contains("logic"))
    }
  }
}
