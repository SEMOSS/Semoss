package prerna.rdf.engine.wrappers;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class CustomSparqlAggregationParser extends QueryModelVisitorBase<Exception> {

	public Set<String> values = new HashSet<String>();

	public Set<String> getValue(){
		return values;
	}

	@Override
	public void meet(Avg node) {
		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
	@Override
	public void meet(Max node) {
		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
	@Override
	public void meet(Min node) {
		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
	@Override
	public void meet(Sum node) {
		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
	@Override
	public void meet(Count node) {
		values.add(node.getArg().getParentNode().getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
	
	@Override
	public void meet(MathExpr node) throws Exception {
		values.add(node.getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
	}
}

