package org.apache.boost

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * Kryo序列化
 *
 * @author Sam Ma
 * @date 2024/12/28
 */
object KryoSerializer {

  // 1 - define a SparkConf object with the Kryo serializer
  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(Array( // 2- register the classes we want to serializer
      classOf[Person],
      classOf[Array[Person]]
    ))

  val spark = SparkSession.builder().appName("Kryo Serialization")
    .config(sparkConf)  // 3- pass the SparkConf object to the SparkSession
    .master("local[*]").getOrCreate()

  val sc = spark.sparkContext
  case class Person(name: String, age: Int)

  def generatePeople(nPersons: Int) = (1 to nPersons)
    .map(i => Person(s"Person$i", i % 100))

  val people = sc.parallelize(generatePeople(10000000))

  def testCaching() = {
    people.persist(StorageLevel.MEMORY_ONLY_SER).count()
    /*
      Java serialization
      - memory usage 254.9 MiB
      - time 39 s

      Kryo serialization
      - memory usage 	189.7 MiB
      - time 31 s
     */
  }

  def testShuffling() = {
    people.map(p => (p.age, p)).groupByKey().mapValues(_.size).count()
    /*
      Java serialization
      - memory usage  shuffle read/ shuffle write: 74.3 MiB
      - time 36 s

      Kryo serialization
      - memory usage  shuffle read/ shuffle write: 49.7 MiB
      - time 25 s
     */
  }

  def main(args: Array[String]): Unit = {
    testShuffling()
    Thread.sleep(10000000)
  }

}
