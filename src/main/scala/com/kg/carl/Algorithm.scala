package com.kg.carl
import com.kg.carl.Utils._
import util.control.Breaks._
import scala.collection.mutable._
import com.kg.carl.Preprocessor._

object Algorithm {

  case class ScoredRule(
    p: Int,
    q: Int,
    r: Int) {
    var support = 0
    var body_support = 0
    var head_coverage = 0.0
    var standard_confidence = 0.0
    var pca_confidence = 0.0
    var completeness_confidence = 0.0
    var precision = 0.0
    var recall = 0.0
    var directional_metric = 0.0
    var directional_coef = 0.0
  }

  val MIN_HEAD_COVERAGE = 0.001
  val MIN_STANDARD_CONFIDENCE = 0.001
  val MIN_SUPPORT = 10
  val CONFIDENCE_INCOMPLETENESS_FACTOR = 0.5

  def mineRulesWithCardinalities(
    pso:   LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, ArrayBuffer[Int]]]],
    nodes: ArrayBuffer[String], id_for_nodes: LinkedHashMap[String, Int],
    properties: TreeSet[Int],
    ecpv:       LinkedHashMap[Int, ArrayBuffer[LinkedHashMap[Int, Int]]],
    numRules:   Int): ListBuffer[ScoredRule] = {
    val entities = Set[Int]()
    val property_instances_count = LinkedHashMap[Int, Int]().withDefaultValue(0)
    val entityCount = getNumberOfEntities(nodes)
    println(entityCount)
    pso.foreach {
      pxy =>
        pxy._2.foreach {
          xy =>
            xy.foreach {
              e =>
                val elem = property_instances_count.get(e._1)
                if (elem == None) {
                  property_instances_count(pxy._1) = property_instances_count(pxy._1) + e._2.size
                }
            }

        }
    }
    val number_of_expected_triple_per_relation = Map[Int, Int]()
    println(number_of_expected_triple_per_relation)
    properties.foreach {
      kv =>
        for (subject <- 0 to entityCount) {
          val expectedCardinality = getExpectedCardinality(ecpv, subject, kv)
          if (expectedCardinality != 0) {
            val actualCardinality = 0
            if (isExisting(pso(kv), subject)) {
              val actualCardinality = getSize(pso(kv), subject)
            }
            if (expectedCardinality > actualCardinality) {
              if (number_of_expected_triple_per_relation.contains(kv)) {
                val m = number_of_expected_triple_per_relation.apply(kv) + (expectedCardinality - actualCardinality)
                number_of_expected_triple_per_relation.put(kv, m)
              } else {
                number_of_expected_triple_per_relation.put(kv, (expectedCardinality - actualCardinality))
              }
            }
          }
        }
    }
    val rules = ListBuffer[ScoredRule]()
    val empty_entity_set = ArrayBuffer[Int]()

    properties.foreach {
      p =>
        properties.foreach {
          q =>
            properties.foreach {
              r =>
                breakable {
                  val rule = new ScoredRule(p, q, r)
                  var pcaSupport = 0.0
                  val facts_added_by_subject_with_cardinality = LinkedHashMap[Int, Int]()
                  pso.get(p).foreach {
                    ab =>
                      ab.foreach {
                        ab2 =>
                          val z_created = ArrayBuffer[Int]()
                          ab2.foreach {
                            xy =>
                              val x = xy._1
                              xy._2.foreach {
                                l =>
                                  var new_z_created = getObjects(pso(q), l)
                                  //pso(q).getOrElse(y._1, empty_entity_set)
                                  //z_created.add(new_z_created.asInstanceOf[Int])
                                  z_created ++= new_z_created
                                //println(z_created)
                              }
                              if (!z_created.isEmpty) {
                                val z_actual = getObjects(pso(r), x)
                                val expects_cardinality = hasExpectedCardinality(ecpv, x, r)
                                rule.body_support += z_created.size
                                if (!z_actual.isEmpty) {
                                  pcaSupport += z_created.size
                                }
                                z_created.foreach {
                                  z =>
                                    if (z_actual.contains(z)) {
                                      rule.support = rule.support + 1
                                    } else if (expects_cardinality) {
                                      if (facts_added_by_subject_with_cardinality.contains(x)) {
                                        val m = facts_added_by_subject_with_cardinality.apply(x) + 1
                                        facts_added_by_subject_with_cardinality.put(x, m)
                                      } else {
                                        facts_added_by_subject_with_cardinality.put(x, 1)
                                      }

                                    }
                                }

                              }
                          }
                      }
                  }
                  if (rule.support < MIN_SUPPORT) {
                    break
                  }
                  rule.head_coverage = rule.support.asInstanceOf[Double] / property_instances_count(rule.r)
                  if (rule.head_coverage < MIN_HEAD_COVERAGE) {
                    break
                  }
                  rule.standard_confidence = rule.support.asInstanceOf[Double] / rule.body_support
                  if (rule.standard_confidence < MIN_STANDARD_CONFIDENCE) {
                    break
                  }
                  rule.pca_confidence = rule.support.asInstanceOf[Double] / pcaSupport
                  var triple_added_to_missing_places_count = 0
                  var triple_added_to_complete_places_count = 0
                  facts_added_by_subject_with_cardinality.foreach {
                    t =>
                      val expected_cardinality = getExpectedCardinality(ecpv, t._1, rule.r)
                      if (expected_cardinality.asInstanceOf[Int] != 0) {
                        val actual_triples_number = pso(rule.r)(t._1).size
                        var missing_triples = 0
                        if (expected_cardinality.asInstanceOf[Int] > actual_triples_number) {
                          missing_triples = expected_cardinality.asInstanceOf[Int] - actual_triples_number
                        }
                        val triples_added_by_the_rule = t._2
                        if (triples_added_by_the_rule > missing_triples) {
                          triple_added_to_missing_places_count += missing_triples
                          triple_added_to_complete_places_count += triples_added_by_the_rule - missing_triples
                        } else {
                          triple_added_to_missing_places_count += triples_added_by_the_rule
                        }
                      } else {
                        println("No cardinality exists but still stored the facts")
                      }
                  }
                  rule.completeness_confidence = rule.support / (rule.body_support - triple_added_to_missing_places_count).asInstanceOf[Double]
                  rule.precision = 1 - triple_added_to_complete_places_count.asInstanceOf[Double] / rule.body_support
                  if (number_of_expected_triple_per_relation.contains(rule.r)) {
                    rule.recall = triple_added_to_missing_places_count.asInstanceOf[Double] / number_of_expected_triple_per_relation(rule.r)
                  } else {
                    rule.recall = Double.NaN
                  }
                  if ((triple_added_to_complete_places_count + triple_added_to_missing_places_count) != 0) {
                    rule.directional_metric =
                      (triple_added_to_missing_places_count - triple_added_to_complete_places_count).asInstanceOf[Double] /
                        (2 * (triple_added_to_missing_places_count + triple_added_to_complete_places_count)) + 0.5
                  } else {
                    rule.directional_metric = Double.NaN
                  }
                  val possible_relations_num = entityCount * entityCount
                  var expected_incomplete = 0
                  var expected_complete = 0
                  if (number_of_expected_triple_per_relation.contains(rule.r)) {
                    expected_incomplete = number_of_expected_triple_per_relation(rule.r) / possible_relations_num
                    expected_complete = (possible_relations_num - number_of_expected_triple_per_relation(rule.r) - property_instances_count(rule.r)) / possible_relations_num
                  }
                  val actual_complete = triple_added_to_complete_places_count.asInstanceOf[Double] / rule.body_support
                  val actual_incomplete = triple_added_to_missing_places_count.asInstanceOf[Double] / rule.body_support
                  if (actual_complete == 0 || expected_incomplete == 0) {
                    rule.directional_coef = Float.MaxValue
                  } else {
                    rule.directional_coef = 0.5 * expected_complete / actual_complete + 0.5 * actual_incomplete / expected_incomplete
                  }
                  rules += rule
                }
            }
        }
    }
    rules.sortWith { (a: ScoredRule, b: ScoredRule) =>
      a.completeness_confidence > b.completeness_confidence
    }
    val limit = scala.math.min(numRules, rules.size)
    val result = ListBuffer[ScoredRule]()
    for (i <- 0 until limit) {
      result += rules(i)
    }
    return result

  }

}
