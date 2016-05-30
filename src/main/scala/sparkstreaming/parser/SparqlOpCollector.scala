package sparkstreaming.parser

import org.openrdf.query.algebra.{ProjectionElem, StatementPattern, Var}

import scala.collection.mutable

/**
  * Created by xiangnanren on 17/05/16.
  */
class SparqlOpCollector(visitor: SparqlOpVisitor) {

  val opMap = new mutable.LinkedHashMap[StatementPattern, String]
  val opProjects = new mutable.ArrayBuffer[ProjectionElem]
  val opSPMap = new mutable.LinkedHashMap[StatementPattern, Int]

  val predicates = new mutable.ArrayBuffer[Var]
  val varWeightMap = new mutable.LinkedHashMap[String, Int]

  private var opId: Int = -1

  visitor.projects.foreach(x => {
    initOpProjects(x); projectsWeight(x)
  })
  visitor.statementPatterns.foreach(x => {
    initPredicates(x); initOpSPsMap(x); spsWeight(x)
  })
  varWeightMap.retain((key, value) => value >= 2)


  def initOpMap() = {

  }

  def initOpProjects(project: ProjectionElem) = {
    opProjects.append(project)
  }

  def projectsWeight(project: ProjectionElem) = {
    varWeightMap.put(project.getTargetName, 1)
  }

  def initPredicates(sp: StatementPattern) = {
    if (sp.getPredicateVar.hasValue) predicates.append(sp.getPredicateVar)
  }

  def initOpSPsMap(sp: StatementPattern) = {
    opId += 1
    opSPMap.put(sp, opId)
  }

  def spsWeight(sp: StatementPattern) = {
    if (!sp.getSubjectVar.hasValue) {
      if (!varWeightMap.contains(sp.getSubjectVar.getName)) varWeightMap.put(sp.getSubjectVar.getName, 0)
      varWeightMap(sp.getSubjectVar.getName) += 1
    }

    if (!sp.getPredicateVar.hasValue) {
      if (!varWeightMap.contains(sp.getPredicateVar.getName)) varWeightMap.put(sp.getPredicateVar.getName, 0)
      varWeightMap(sp.getPredicateVar.getName) += 1
    }

    if (!sp.getObjectVar.hasValue) {
      if (!varWeightMap.contains(sp.getObjectVar.getName)) varWeightMap.put(sp.getObjectVar.getName, 0)
      varWeightMap(sp.getObjectVar.getName) += 1
    }
  }
}

object SparqlOpCollector {
  def apply(visitor: SparqlOpVisitor): SparqlOpCollector = new SparqlOpCollector(visitor)

}