package com.kg.carl

import java.net.URL
import org.apache.spark.util._
import scala.collection.SortedSet
import scala.collection.mutable._
import org.apache.spark.SparkContext
import scala.math

object Utils {

  def getNodeForId(nodes: ArrayBuffer[String], 
      id: Int): String = {
    
    return nodes(id)
  }

  def getIdForNode(
    nodes:        CollectionAccumulator[ArrayBuffer[String]],
    idNodes: CollectionAccumulator[LinkedHashMap[String, Int]],
    node:         String): Int = {

    if (idNodes.value.size() != 0) {
      if (!(idNodes.value.get(0).contains(node))) {
        idNodes.value.get(0) += node -> nodes.value.get(0).size
        nodes.value.get(0) += node
      }
    } else {
      idNodes.add(LinkedHashMap(node -> 0))
      nodes.add(ArrayBuffer(node))
    }

    return idNodes.value.get(0)(node)
  }

  def getIdForNode(
    nodes:        ListBuffer[String],
    idNodes: LinkedHashMap[String, Int],
    node:         String): Int = {

    if (!(idNodes.contains(node))) {
      idNodes(node) = nodes.size
      nodes += node
    }

    return idNodes(node)
  }

  def getNumberOfEntities(nodes: ArrayBuffer[String]): Int = {
    
    return nodes.size
  }

  def contains(
    pso:          LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]],
    nodes:        ListBuffer[String],
    idNodes: LinkedHashMap[String, Int],
    subject:      String,
    predicate:    String,
    `object`:     String): Boolean = {

    return contains(pso, getIdForNode(nodes, idNodes, subject),
      getIdForNode(nodes, idNodes, predicate),
      getIdForNode(nodes, idNodes, `object`))
  }

  def contains(
    pso:       LinkedHashMap[Int, LinkedHashMap[Int, Set[Int]]],
    subject:   Int,
    predicate: Int,
    `object`:  Int): Boolean = {

    return (pso(predicate).contains(subject) &&
      pso(predicate)(subject).contains(`object`))
  }

  def hasExpectedCardinality(
    ecpv: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    s:    Int,
    p:    Int): Boolean = {
    
    val arrBuf = ecpv.get(p)
    arrBuf.foreach {
      iterator =>
        iterator.foreach {
          map =>
            if (map.contains(s)) {
              return true
            }
        }
    }
    return false
  }

  def getExpectedCardinality(
    ecpv: LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    s:    Int,
    p:    Int): Int = {

    var default = 0
    val arrBuf = ecpv.get(p)
    arrBuf.foreach {
      iterator =>
        iterator.foreach {
          map =>
            if (map.contains(s)) {
              return map.apply(s)
            }
        }
    }
    return default
  }

  def isExisting(
    arrBuf:     ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    subject: Int): Boolean = {
    
    arrBuf.foreach {
      iterator =>
        if (iterator.contains(subject)) {
          return true
        }
    }
    return false
  }

  def getSize(
    arrBuf:     ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    subject: Int): Int = {
    
    arrBuf.foreach {
      iterator =>
        if (iterator.contains(subject)) {
          return iterator.get(subject).size
        }
    }
    return 0
  }
  
  def getObjects(
    arrBuf: ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]],
    q:   Int): ArrayBuffer[Int] = {
    val empty = ArrayBuffer[Int]()
    arrBuf.foreach {
      i =>
        if (i.contains(q)) {
          return i.apply(q)
        }
    }
    return empty
  }

  def addOrUpdate[K, V](m: LinkedHashMap[K, V], k: K, kv: (K, V),
                        f: V => V) {
    
    m.get(k) match {
      case Some(e) => m.update(k, f(e))
      case None    => m += kv
    }
  }

  def parseTriples(parsData: String): Triples = {
    
    val spo = parsData.split("\t")
    val subject = spo(0)
    val predicate = spo(1)
    val `object` = spo(2)

    Triples(subject, predicate, `object`)
  }

  def parseCardinalities(parsData: String): Cardinalities = {
    
    val spo = parsData.split("\t")
    val indexOfDel = spo(0).indexOf('|')
    if (spo(1) == "hasExactCardinality") {
      val subject = spo(0).substring(0, indexOfDel)
      val predicate = spo(0).substring(indexOfDel + 1, spo(0).length())
      val cardinality = spo(2).toInt

      Cardinalities(subject, predicate, cardinality)
    } else {
      Cardinalities("NA", "NA", 0)
    }

  }

  case class Triples(
    subject:   String,
    predicate: String,
    obj:       String)

  case class Cardinalities(
    subject:     String,
    predicate:   String,
    cardinality: Int)

}
