package com.kg.carl

import java.net.URI
import java.io._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable._
import scala.collection.mutable.{ Set, Map }
import java.nio.file.Files;
import java.nio.file.Paths
import com.kg.carl.Algorithm._
import org.apache.spark.util._
import com.kg.carl.IdStore._

object Preprocessor {

  /**
   * @param args
   */
  def main(args: Array[String]) {
    run()
  }

  def run(): Unit = {
    val spark = SparkSession.builder
      .appName(s"CARL")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext
    println("=============================================")
    println("|                CARL-Scala                 |")
    println("=============================================")

    //val files = input.split(" ")

    val triples = sc.textFile("/home/hduser/spark/CARL-KG/src/main/resources/newa.tsv")
    val cardinalities = sc.textFile("/home/hduser/spark/CARL-KG/src/main/resources/newb.tsv")

    val parsedTriples = triples.map(parseTriples)

    println("Done Parsing Input Triples")

    println("Number of Triples parsed: " + parsedTriples.count())

    parsedTriples.take(5).foreach(println(_))

    println("=============================================")

    val parsedCardinalities = cardinalities.map(parseCardinalities)
    println("Done Parsing Input Cardinalities")

    println("Number of Cardinalities parsed: " + parsedCardinalities.count())

    parsedCardinalities.take(5).foreach(println(_))

    var i = 0
    import spark.implicits._

    val AccNodes = sc.collectionAccumulator[ListBuffer[String]]
    val AccIdNodes = sc.collectionAccumulator[LinkedHashMap[String, Int]]
    val AccPSO = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]]]]
    val AccProperties = sc.collectionAccumulator[TreeSet[Int]]
    val AccECPV = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]]]

    parsedTriples.take(parsedTriples.count().asInstanceOf[Int]) foreach { spo: Triples =>

      val s = getIdForNode(AccNodes, AccIdNodes, spo.subject)
      val p = getIdForNode(AccNodes, AccIdNodes, spo.predicate)
      val o = getIdForNode(AccNodes, AccIdNodes, spo.obj)

      val set1 = ArrayBuffer[Int]()
      val m = ArrayBuffer(LinkedHashMap(s -> set1.+=:(o)))
      if (AccPSO.value.size() == 0)
        AccPSO.add(LinkedHashMap(p -> m))
      else {
        if (AccPSO.value.get(0).contains(p)) {
          if (AccPSO.value.get(0).apply(p).contains(s)) {
            val k = AccPSO.value.get(0).apply(p).apply(s)
            val se = ArrayBuffer[Int]()
            k += s -> se.+=:(o)
          } else {
            val k = AccPSO.value.get(0).apply(p)

            k += LinkedHashMap(s -> ArrayBuffer(o))
          }

        } else {
          AccPSO.value.get(0).put(p, m)
        }
      }

      if (AccProperties.value.size() == 0)
        AccProperties.add(TreeSet(p))
      else
        AccProperties.value.get(0) += p

      i = i + 1
      if (i % 1000000 == 0) {
        print("*")
      }

    }
    println("=============================================")
    println("|             Cardinality Rule Mining       |")
    println("=============================================")
    parsedCardinalities.take(parsedTriples.count().asInstanceOf[Int]) foreach { spo: Cardinalities =>
      val s = getIdForNode(AccNodes, AccIdNodes, spo.subject)
      val p = getIdForNode(AccNodes, AccIdNodes, spo.predicate)

      if (AccECPV.value.size() == 0)
        AccECPV.add(LinkedHashMap(p -> ArrayBuffer(LinkedHashMap(s -> spo.cardinality))))
      else {
        if (AccECPV.value.get(0).contains(p)) {
          val k = AccECPV.value.get(0).apply(p)
          k += LinkedHashMap(s -> spo.cardinality)
        } else
          AccECPV.value.get(0).put(p, ArrayBuffer(LinkedHashMap(s -> spo.cardinality)))

      }
    }

    val nodes = AccNodes.value.get(0)
    val id_for_nodes = AccIdNodes.value.get(0)
    val properties = AccProperties.value.get(0)

    val pso = AccPSO.value.get(0)

    val expected_cardinalities_by_property_value = AccECPV.value.get(0)

    println(pso.size)
    val output = doMining(pso, nodes, id_for_nodes, properties, expected_cardinalities_by_property_value, 1000)
    val file = new File("output.tsv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("p\tq\tr\tsupport\tbody support\thead coverage\tstd conf\tpca conf\tcompl conf\tprecision\trecall\tdir metric\tdir coef\trule eval\n")
    output.foreach{
      rule=>
        bw.write( getNodeForId(nodes,rule.p) + "\t" + getNodeForId(nodes,rule.q)
                          + "\t" + getNodeForId(nodes,rule.r) + "\t" + rule.support + "\t"
                          + rule.body_support + "\t" + rule.head_coverage + "\t"
                          + rule.standard_confidence + "\t" + rule.pca_confidence + "\t"
                          + rule.completeness_confidence + "\t"
                          + rule.precision + "\t" + rule.recall + "\t"
                          + rule.directional_metric + "\t" + rule.directional_coef + "\t" + "\n")
    }
    bw.close()
    spark.stop

  }
  //def flip[X, Y](m: Map[X, Y]): Map[Y, Set[X]] = m.groupBy(_._2).map(e => e._1 -> e._2.map(_._1).toSet)

  def getNodeForId(nodes: ListBuffer[String], id: Int): String = {
    return nodes(id)
  }

  def getIdForNode(nodes: CollectionAccumulator[ListBuffer[String]], id_for_nodes: CollectionAccumulator[LinkedHashMap[String, Int]], node: String): Int = {
    if (id_for_nodes.value.size() != 0) {
      if (!(id_for_nodes.value.get(0).contains(node))) {
        id_for_nodes.value.get(0) += node -> nodes.value.get(0).size
        nodes.value.get(0) += node
      }
    } else {
      id_for_nodes.add(LinkedHashMap(node -> 0))
      nodes.add(ListBuffer(node))
    }
    return id_for_nodes.value.get(0)(node)
  }

  def getIdForNode(nodes: ListBuffer[String], id_for_nodes: LinkedHashMap[String, Int], node: String): Int = {
    if (!(id_for_nodes.contains(node))) {
      id_for_nodes(node) = nodes.size
      nodes += node
    }
    return id_for_nodes(node)
  }

  def getNumberOfEntities(nodes: ListBuffer[String]): Int = {
    return nodes.size
  }

  def getProperties(properties: Set[Int]): Set[Int] = {
    return properties
  }

  def contains(pso: LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]], nodes: ListBuffer[String], id_for_nodes: LinkedHashMap[String, Int], subject: String, predicate: String, `object`: String): Boolean = {
    return contains(pso, getIdForNode(nodes, id_for_nodes, subject), getIdForNode(nodes, id_for_nodes, predicate), getIdForNode(nodes, id_for_nodes, `object`))
  }

  def contains(pso: LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]], subject: Int, predicate: Int, `object`: Int): Boolean = {
    return (pso(predicate).contains(subject) && pso(predicate)(subject).contains(`object`))
  }

  def hasExpectedCardinality(expected_cardinalities_by_property_value: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]], s: Int, p: Int): Boolean = {
    val iter = expected_cardinalities_by_property_value.get(p)
    iter.foreach {
      i =>
        i.foreach {
          e =>
            if (e.contains(s)) {
              return true
            }
        }
    }
    return false
  }

  def getExpectedCardinality(expected_cardinalities_by_property_value: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]], s: Int, p: Int): Int = {

    var ret = 0
    val iter = expected_cardinalities_by_property_value.get(p)
    iter.foreach {
      i =>
        i.foreach {
          e =>
            if (e.contains(s)) {
              return e.apply(s)
            }
        }
    }
    return ret
  }

  def isExisting(map: ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]], subject: Int): Boolean = {
    map.foreach {
      i =>
        if (i.contains(subject)) {
          return true
        }
    }
    return false
  }
  def getSize(map: ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]], subject: Int): Int = {
    map.foreach {
      i =>
        if (i.contains(subject)) {

          return i.get(subject).size
        }
    }
    return 0
  }
  def getObjects(map: ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]], q: Int): ArrayBuffer[Int] = {
    val empty = ArrayBuffer[Int]()
    map.foreach {
      i =>
        if (i.contains(q)) {

          return i.apply(q)
        }
    }
    return empty
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Preprocessor") {

    head("CARL")

    opt[String]('i', "input").required().valueName("<paths>").
      action((x, c) => c.copy(in = x)).
      text("2 tsv file paths required First file should contain triples and the second should contain the cardinalities ")

    help("help").text("for more info")
  }
}
