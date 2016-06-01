package com.advancedspark.ch1intro

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

import java.lang.Double.isNaN

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

object DataLinkage extends Serializable {
  def main(args: Array[String]): Unit = {
    //input file
    val inputPath = "/Users/Documents/Advancedspark/ch1/Datalinkage/block_1.csv"

    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.broadcast.compress", "false")
    sparkConfig.set("spark.shuffle.compress", "false")
    sparkConfig.set("spark.shuffle.spill.compress", "false")
    val sc = new SparkContext("local", "DataLinkage", sparkConfig)

    val rawblocks = sc.textFile(inputPath)
    //checking the counts     val counte = rawblocks.count()     print(counte)
    //val headerprint = rawblocks.filter(isHeader)
    //headerprint.foreach(println)
    //"id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"

    //function to remove header
    def isHeader(line: String): Boolean = {
      line.contains("id_1")
    }

    val lines = rawblocks.filter(x => !isHeader(x))

    def toDouble(s: String) = {
      if ("?".equals(s)) Double.NaN else s.toDouble
    }

    //parse line also write toDouble function inorder to remove "?" from the sliced data 
    //add a case class before the object
    def parse(line: String) = {
      val pieces = line.split(",")
      val id1 = pieces(0).toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      MatchData(id1, id2, scores, matched)
    }

    val parsed = lines.map(line => parse(line))

    val grouped = parsed.groupBy(md => md.matched)
    //val gcount = grouped.mapValues(x => x.size).foreach(println)
    val matchCounts = parsed.map(md => md.matched).countByValue()
    //val matchCountsSeq = matchCounts.toSeq
    // matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    val statistics = parsed.map(md => md.scores(0)).stats() //nothing displayed
    val StatsNoNaN = parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()
    print(StatsNoNaN)

    //Creating summary Stats for all the values in Array[Double]
    val stats = (0 until 9).map(i => {
      parsed.map(_.scores(i)).filter(!_.isNaN).stats()
    })

    //print(stats)

    //Testing NAStatCounter
    val arr = Array(1.0, Double.NaN, 17.29)
    val nas = arr.map(x => NAStatCounter(x))
    //nas.foreach(println)

    val nasRDD = parsed.map(md => { md.scores.map(d => NAStatCounter(d)) })

    //Writing the reduce Function
    //TestCase
    val nas1 = Array(1.0, Double.NaN).map(d => NAStatCounter(d))
    val nas2 = Array(Double.NaN, 2.0).map(d => NAStatCounter(d))

    //Method1 for merging:
    //val merged = nas1.zip(nas2).map(p => p._1.merge(p._2))
    //Method2 for merging:
    //val merged2 = nas1.zip(nas2).map {case(a,b) => a.merge(b) }
    val nased = List(nas1, nas2)

    //reduce
    val merged = nased.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
    //END of Test case

    val reduced = nasRDD.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    statsm.zip(statsn).map {
      case (m, n) =>
        (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d

    case class Scored(md: MatchData, score: Double)

    //changing NaN values to 0 scoring in class
    val ct = parsed.map(md => {
      val score = Array(2, 4, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    ct.filter(s => s.score >= 4.0).
      map(s => s.md.matched).countByValue().foreach(println)
    ct.filter(s => s.score >= 2.0).
      map(s => s.md.matched).countByValue().foreach(println)

  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => { n1.zip(n2).map { case (a, b) => a.merge(b) } })
  }
}

//NAStatCounter Class Implementation
class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0
  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }
  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }
  override def toString = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}
object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
