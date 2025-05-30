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

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3._
import software.amazon.awssdk.services.s3.model._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object S3Utils extends Logging {

  // === Load Spark config values from SparkConf via SparkEnv ===
  private def getConf(key: String, default: String): String = {
    Option(SparkEnv.get).flatMap(env => Option(env.conf.getOption(key).orNull)).getOrElse(default)
  }

  lazy val s3Client: S3Client = {
    val builder = S3Client.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())

    val endpointOpt = sys.env.get("SPARK_S3_ENDPOINT")
    endpointOpt match {
      case Some(endpoint) =>
        builder.endpointOverride(java.net.URI.create(endpoint))
        builder.region(Region.US_EAST_1)
      case None =>
        builder.region(Region.of(getConf("spark.s3.region", "us-east-1")))
    }

    builder.build()
  }

  // === Retry block ===
  def retry[T](retries: Int, backoffMs: Long)(block: => T): T = {
    var attempts = 0
    var delay = backoffMs
    while (true) {
      try return block
      catch {
        case e if attempts < retries =>
          logWarning(s"Retrying after failure: ${e.getMessage}")
          Thread.sleep(delay)
          delay *= 2
          attempts += 1
        case e: Throwable =>
          throw e
      }
    }
    throw new RuntimeException("Unexpected execution path in retry block")
  }

  // === Write Partition to S3 ===
  def writePartition[T](bucket: String, prefix: String, partId: Int, data: Iterator[T]): String = {
    val compress = getConf("spark.s3.offload.compress", "false").toBoolean
    val retries = getConf("spark.s3.offload.retries", "3").toInt
    val backoff = getConf("spark.s3.offload.backoff.ms", "100").toLong

    val path = s"$prefix/part-$partId"
    val byteStream = new ByteArrayOutputStream()

    val outStream: ObjectOutputStream = {
      val baseStream = if (compress) new GZIPOutputStream(byteStream) else byteStream
      new ObjectOutputStream(baseStream)
    }

    try {
      data.foreach(outStream.writeObject)
      outStream.flush()
      if (compress) outStream.asInstanceOf[GZIPOutputStream].finish()

      val bytes = byteStream.toByteArray
      val req = PutObjectRequest.builder()
        .bucket(bucket)
        .key(path)
        .build()

      retry(retries, backoff) {
        s3Client.putObject(req, RequestBody.fromBytes(bytes))
      }

      s"s3://$bucket/$path"
    } finally {
      outStream.close()
      byteStream.close()
    }
  }

  // === Read all partition files from S3 ===
  def readPartitions[T: ClassTag](bucket: String, uris: Array[String]): Array[T] = {
    val compress = getConf("spark.s3.offload.compress", "false").toBoolean
    val retries = getConf("spark.s3.offload.retries", "3").toInt
    val backoff = getConf("spark.s3.offload.backoff.ms", "100").toLong

    val buffer = ArrayBuffer[T]()

    uris.foreach { uri =>
      val key = uri.stripPrefix(s"s3://$bucket/")
      val req = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      val stream = retry(retries, backoff) {
        s3Client.getObject(req)
      }

      val objStream = {
        val base = if (compress) new GZIPInputStream(stream) else stream
        new ObjectInputStream(base)
      }

      try {
        while (true) {
          val obj = objStream.readObject().asInstanceOf[T]
          buffer += obj
        }
      } catch {
        case _: EOFException => // end of stream
      } finally {
        objStream.close()
        stream.close()
      }
    }

    buffer.toArray
  }

  // === Cleanup helper ===
  def deletePrefix(bucket: String, prefix: String): Unit = {
    val listReq = ListObjectsV2Request.builder()
      .bucket(bucket)
      .prefix(prefix)
      .build()

    val objects = retry(3, 100) {
      s3Client.listObjectsV2(listReq).contents().asScala
    }

    objects.foreach { obj =>
      val delReq = DeleteObjectRequest.builder()
        .bucket(bucket)
        .key(obj.key())
        .build()
      retry(3, 100) {
        s3Client.deleteObject(delReq)
      }
    }
  }
}
