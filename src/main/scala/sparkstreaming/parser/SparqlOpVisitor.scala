package sparkstreaming.parser

import org.openrdf.query.algebra._
import org.openrdf.query.algebra.helpers.AbstractQueryModelVisitor

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 12/05/16.
  */
class SparqlOpVisitor(tableName: String) extends AbstractQueryModelVisitor[RuntimeException] {

  var projects: List[ProjectionElem] = Nil
  var statementPatterns: List[StatementPattern] = Nil


  //  override def meet(node: QueryRoot) {}
  //
  //  override def meet(add: Add) {}
  //
  //  override def meet(node: And) {}
  //
  //  override def meet(node: ArbitraryLengthPath) {}
  //
  //  override def meet(node: Avg) {}
  //
  //  override def meet(node: BindingSetAssignment) {}
  //
  //  override def meet(node: BNodeGenerator) {}
  //
  //  override def meet(node: Bound) {}
  //
  //  override def meet(clear: Clear) {}
  //
  //  override def meet(node: Coalesce) {}
  //
  //  override def meet(node: Compare) {}
  //
  //  override def meet(node: CompareAll) {}
  //
  //  override def meet(node: CompareAny) {}
  //
  //  override def meet(node: DescribeOperator) {}
  //
  //  override def meet(copy: Copy) {}
  //
  //  override def meet(node: Count) {}
  //
  //  override def meet(create: Create) {}
  //
  //  override def meet(node: Datatype) {}
  //
  //  override def meet(deleteData: DeleteData) {}
  //
  //  override def meet(node: Difference) {}
  //
  //  override def meet(node: Distinct) {}
  //
  //  override def meet(node: EmptySet) {}
  //
  //  override def meet(node: Exists) {}
  //
  //  override def meet(node: Extension) {}
  //
  //  override def meet(node: ExtensionElem) {}
  //
  //  override def meet(node: Filter) {}
  //
  //  override def meet(node: FunctionCall) {}
  //
  //  override def meet(node: Group) {}
  //
  //  override def meet(node: GroupConcat) {}
  //
  //  override def meet(node: GroupElem) {}
  //
  //  override def meet(node: If) {}
  //
  //  override def meet(node: In) {}
  //
  //  override def meet(insertData: InsertData) {}
  //
  //  override def meet(node: Intersection) {}
  //
  //  override def meet(node: IRIFunction) {}
  //
  //  override def meet(node: IsBNode) {}
  //
  //  override def meet(node: IsLiteral) {}
  //
  //  override def meet(node: IsNumeric) {}
  //
  //  override def meet(node: IsResource) {}
  //
  //  override def meet(node: IsURI) {}
  //
  //  override def meet(node: Join) {}
  //
  //  override def meet(node: Label) {}
  //
  //  override def meet(node: Lang) {}
  //
  //  override def meet(node: LangMatches) {}
  //
  //  override def meet(node: LeftJoin) {}
  //
  //  override def meet(node: Like) {}
  //
  //  override def meet(node: Load) {}
  //
  //  override def meet(node: LocalName) {}
  //
  //  override def meet(node: MathExpr) {}
  //
  //  override def meet(node: Max) {}
  //
  //  override def meet(node: Min) {}
  //
  //  override def meet(modify: Modify) {}
  //
  //  override def meet(move: Move) {}
  //
  //  override def meet(node: MultiProjection) {}
  //
  //  override def meet(node: Namespace) {}
  //
  //  override def meet(node: Not) {}
  //
  //  override def meet(node: Or) {}
  //
  //  override def meet(node: Order) {}
  //
  //  override def meet(node: OrderElem) {}
  //
  //  override def meet(node: Projection) {}
  //
  //  override def meet(node: ProjectionElem) {}

  override def meet(node: ProjectionElemList): Unit = {
    projects = node.getElements.toList
  }

  //  override def meet(node: Reduced) {}
  //
  //  override def meet(node: Regex) {}
  //
  //  override def meet(node: SameTerm) {}
  //
  //  override def meet(node: Sample) {}
  //
  //  override def meet(node: Service) {}
  //
  //  override def meet(node: SingletonSet) {}
  //
  //  override def meet(node: Slice) {}

  override def meet(node: StatementPattern): Unit = {
    statementPatterns = statementPatterns :+ node
  }


  //
  //  override def meet(node: Str) {}
  //
  //  override def meet(node: Sum) {}
  //
  //  override def meet(node: Union) {}
  //
  //  override def meet(node: ValueConstant) {}

  /**
    * @since 2.7.4
    */
  //  override def meet(node: ListMemberOperator) {}
  //
  //  override def meet(node: Var) {}
  //
  //  override def meet(node: ZeroLengthPath) {}
  //
  //  override def meetOther(node: QueryModelNode) {}

}


object SparqlOpVisitor {
  def apply(tableName: String) = new SparqlOpVisitor(tableName: String)
}
