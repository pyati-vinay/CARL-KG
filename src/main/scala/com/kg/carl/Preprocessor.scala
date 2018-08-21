package com.kg.carl

import java.net.URI
import java.io._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable._
import java.nio.file.Files;
import java.nio.file.Paths
import com.kg.carl.Algorithm._
import org.apache.spark.util._
import com.kg.carl.Utils._

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
      
    import spark.implicits._
    val sc = spark.sparkContext
    
    println("=============================================")
    println("|                CARL-Scala                 |")
    println("=============================================")

    //val files = input.split(" ")

    val triples = sc.textFile("/home/hduser/spark/CARL-KG/src/main/resources/test.tsv")
    val cardinalities = sc.textFile("/home/hduser/spark/CARL-KG/src/main/resources/newb.tsv")

    val parsedTriples = triples.map(parseTriples).sortBy(_.predicate)

    println("Number of Triples parsed: " + parsedTriples.count())

    println("=============================================")

    val parsedCardinalities = cardinalities.map(parseCardinalities).sortBy(_.predicate)

    println("Number of Cardinalities parsed: " + parsedCardinalities.count())

    var i = 0

    val AccNodes = sc.collectionAccumulator[ArrayBuffer[String]]
    val AccIdNodes = sc.collectionAccumulator[LinkedHashMap[String, Int]]
    val AccPSO = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]]]]
    val AccProperties = sc.collectionAccumulator[TreeSet[Int]]
    val AccECPV = sc.collectionAccumulator[LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]]]

    parsedTriples.take(parsedTriples.count().asInstanceOf[Int]).foreach { spo: Triples =>

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
    parsedCardinalities.take(parsedTriples.count().asInstanceOf[Int]).foreach { spo: Cardinalities =>
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
    val idNodes = AccIdNodes.value.get(0)
    val properties = AccProperties.value.get(0)
    val pso = AccPSO.value.get(0)
    val ecpv = AccECPV.value.get(0)

    val output = mineRulesWithCardinalities(pso, nodes, idNodes, properties, ecpv, 1000)
    
    val file = new File("output.tsv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("p\tq\tr\tsupport\tbody support\thead coverage\tstd conf\tpca conf\tcompl conf\tprecision\trecall"
            +"\tdir metric\tdir coef\trule eval\n")
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
  
  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Preprocessor") {

    head("CARL")

    opt[String]('i', "input").required().valueName("<paths>").
      action((x, c) => c.copy(in = x)).
      text("2 tsv file paths required First file should contain triples and the second should contain the cardinalities ")

    help("help").text("for more info")
  }
}
