package lowapi

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Date
import java.time.{LocalDate, Period}


/**
 * DStream Transformation
 *
 * @author Sam Ma
 * @date 2024/06/08
 */
object DTransformation {

  val spark = SparkSession.builder()
    .appName("DStreams Transformation")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /* 数据集dateset:
    1:Pennie:Carry:Hirschmann:F:1955-07-02:981-43-9345:56172
    2:An:Amira:Cowper:F:1992-02-08:978-97-8086:40203
    3:Quyen:Marlen:Dome:F:1970-10-11:957-57-8246:53417
    4:Coralie:Antonina:Marshal:F:1990-04-11:963-39-4885:94727
   */
  def readPeople() = ssc.socketTextStream("localhost", 9999).map { line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt,  // id
      tokens(1),  // first name
      tokens(2),  // middle name
      tokens(3),  // last name
      tokens(4),  // gender
      Date.valueOf(tokens(5)),  // birth
      tokens(6),  // ssn
      tokens(7).toInt // salary
    )

  }

  // map、flatmap, filter
  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames(): DStream[String] = readPeople().flatMap { people =>
    List(people.firstName, people.lastName)
  }

  def main(args: Array[String]): Unit = {
    val stream = peopleSmallNames()
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
