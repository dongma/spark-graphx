package rdd.transfrom

import generator.Generator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by madong on 2024/9/14.
 */
object ReusingObjects {

  val spark = SparkSession.builder()
    .appName("Reusing Jvm objects")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  /*
  Analyze text:
    Receive batches of text from data sources, "35 // some text"
    Stats per each data source id:
    - the number of lines to total
    - total number of words in total
    - length of the longest word
    - the number of occurrences of the word "imperdiet"
    Results should be VERY FIRST
   */

  val textPath = "example-data/rtjvm/lipsum/3m.txt"
  val criticalWord = "imperdiet"

  val text = sc.textFile(textPath).map {line =>
    val tokens = line.split("//")
    (tokens(0), tokens(1))
  }

  def generateData() = {
    Generator.generateText(textPath, 60000000, 3000000,200)
  }

  ///// version 1
  case class TextStatus(nLines: Int, nWords: Int, maxWordLength: Int, occurrences: Int)

  object TextStatus {
    val zero = TextStatus(0, 0, 0, 0)
  }

  def callStats() = {
    def aggregateNewRecord(textStatus: TextStatus, record: String): TextStatus = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)

      TextStatus(
        textStatus.nLines + 1,
        textStatus.nWords + newWords.length,
        if (longestWord.length > textStatus.maxWordLength) longestWord.length else textStatus.maxWordLength,
        textStatus.occurrences + newOccurrences
      )
    }

    def combineStats(stats1: TextStatus, stats2: TextStatus): TextStatus = {
      TextStatus(
        stats1.nLines + stats2.nLines,
        stats1.nWords + stats2.nWords,
        if (stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength,
        stats1.occurrences + stats2.occurrences
      )
    }

    val aggregate: RDD[(String, TextStatus)] = text.aggregateByKey(TextStatus.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()
  }

  /// version 2
  class MutableTextStats(var nLines: Int, var nWords: Int,
                          var maxWordLength: Int, var occurrences: Int) extends Serializable

  object MutableTextStats extends Serializable {
    def zero = new MutableTextStats(0, 0, 0, 0)
  }

  def callStatus2() = {
    def aggregateNewRecord(textStats: MutableTextStats, record: String): MutableTextStats = {
      val newWords = record.split(" ")
      val longestWord = newWords.maxBy(_.length)
      val newOccurrences = newWords.count(_ == criticalWord)

      textStats.nLines += 1
      textStats.nWords += newWords.length
      textStats.maxWordLength = if (longestWord.length > textStats.maxWordLength) longestWord.length else textStats.maxWordLength
      textStats.occurrences += newOccurrences

      textStats
    }

    def combineStats(stats1: MutableTextStats, stats2: MutableTextStats): MutableTextStats = {
      stats1.nLines += stats2.nLines
      stats1.nWords += stats2.nWords
      stats1.maxWordLength = if (stats1.maxWordLength > stats2.maxWordLength) stats1.maxWordLength else stats2.maxWordLength
      stats1.occurrences += stats2.occurrences

      stats1
    }

    val aggregate: RDD[(String, MutableTextStats)] = text.aggregateByKey(MutableTextStats.zero)(aggregateNewRecord, combineStats)
    aggregate.collectAsMap()

  }

  // version-3 JVM arrays
  object UglyTextStats extends Serializable {
    val nLinesIndex = 0
    val nWordsIndex = 1
    val longestWordIndex = 2
    val occurrencesIndex = 3

    def aggregateNewRecord(textStats: Array[Int], record: String): Array[Int] = {
      val newWords = record.split(" ") // Array of strings

      var i = 0
      while (i < newWords.length) {
        val word = newWords(i)
        val wordLength = word.length

        textStats(longestWordIndex) = if (wordLength > textStats(longestWordIndex)) wordLength else textStats(longestWordIndex)
        textStats(occurrencesIndex) += (if (word == criticalWord) 1 else 0)

        i += 1
      }

      textStats(nLinesIndex) += 1
      textStats(nWordsIndex) += newWords.length

      textStats
    }

    def combineStats(stats1: Array[Int], stats2: Array[Int]): Array[Int] = {
      stats1(nLinesIndex) += stats2(nLinesIndex)
      stats1(nWordsIndex) += stats2(nWordsIndex)
      stats1(longestWordIndex) = if (stats1(longestWordIndex) > stats2(longestWordIndex)) stats1(longestWordIndex) else stats2(longestWordIndex)
      stats1(occurrencesIndex) += stats2(occurrencesIndex)

      stats1
    }
  }

  def collectStats3() = {
    val aggregate: RDD[(String, Array[Int])] = text.aggregateByKey(Array.fill(4)(0))(UglyTextStats.aggregateNewRecord, UglyTextStats.combineStats)
    aggregate.collectAsMap()
  }

  def main(args: Array[String]): Unit = {
    callStats()
    callStatus2()
    collectStats3()

    Thread.sleep(1000000)
  }

}
