package prerna.rdf.util;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.LocalName;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.VarNameCollector;

public class StatementCollector extends QueryModelVisitorBase<Exception> {
	private List<StatementPattern> statementPatterns = new Vector();
	public Hashtable<String, String> sourceTargetHash = new Hashtable<String, String>();
	public Hashtable constantHash = new Hashtable();
	public Hashtable<String, String> targetSourceHash = new Hashtable<String, String>();
	private List<ProjectionElem> projections = new Vector();

	@Override
	public void meet(StatementPattern node) {
		statementPatterns.add(node);
		try {
			// super.meet(node);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void meet(ProjectionElem node) {
		// System.out.println("Projection is  " +node.getSourceName() +
		// node.getTargetName());
		String target = node.getTargetName();
		// Source target happens when you do ?source As ?target
		// so your query refers to it as source
		if (target != null) {
			sourceTargetHash.put(node.getSourceName(), node.getTargetName());
			targetSourceHash.put(node.getTargetName(), node.getSourceName());
		} else
			sourceTargetHash.put(node.getSourceName(), node.getSourceName());
		// projections.add(node);
	}

	public void meet(ExtensionElem node) {
		System.out.println("Extension Elem is  " + node.getName());
		String target = node.getName();
		// extension element can be one of many
		// it could be a coalesce
		VarNameCollector collector = new VarNameCollector();
		node.visit(collector);
		String source = node.getName();
		Iterator it = collector.getVarNames().iterator();
		while (it.hasNext())
			source = (String) it.next();

		if (!sourceTargetHash.containsKey(source))
			sourceTargetHash.put(source, target);

		System.out.println("Yoo hoo" + collector.getVarNames());

		// it could be a value constant
		ValueConstantCollector collector2 = new ValueConstantCollector();
		try {
			node.visit(collector2);
			constantHash.put(source, collector2.value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// projections.add(node);
	}

	@Override
	public void meet(Coalesce node) {
		// System.out.println("Coalesce is  " + node.getArguments().get(0) );
		// System.out.println("Parent " + node.getParentNode());
		try {
			VarNameCollector collector = new VarNameCollector();
			node.visit(collector);
			System.out.println("Yoo hoo" + collector.getVarNames());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void meet(LocalName constant) {
		// System.out.println("Constants is " + constant);
	}

	// @Override
	public void meet2(ExtensionElem node) {
		System.out.println("Extension Element is  " + node);
		System.out.println(node.getName());
		System.out.println(node.getExpr());
	}

	public List<StatementPattern> getPatterns() {
		return this.statementPatterns;
	}
}