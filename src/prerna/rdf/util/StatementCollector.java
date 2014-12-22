/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.util;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.LocalName;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.VarNameCollector;

public class StatementCollector extends QueryModelVisitorBase<Exception> {
	
	static final Logger logger = LogManager.getLogger(StatementCollector.class.getName());
	private List<StatementPattern> statementPatterns = new Vector();
	public Hashtable<String, String> sourceTargetHash = new Hashtable<String, String>();
	public Hashtable constantHash = new Hashtable();
	public Hashtable<String, String> targetSourceHash = new Hashtable<String, String>();
	private List<ProjectionElem> projections = new Vector();
//	Hashtable<String, String> typeHash = new Hashtable<String, String>(); //variable name --> variable type
//	Hashtable<String, String> subpropHash = new Hashtable<String, String>(); //variable name --> variable type
	Set<String> subjectVariables = new HashSet<String>();//keep track of variables that are subjects
	StringBuffer subjectURIstring = new StringBuffer("");
	Set<String> predicateVariables = new HashSet<String>();//keep track of variables that are predicates
	StringBuffer predicateURIstring = new StringBuffer("");
	Set<String> objectVariables = new HashSet<String>();//keep track of variables that are objects
	StringBuffer objectURIstring = new StringBuffer("");

	@Override
	public void meet(StatementPattern node) {
		//System.out.println("here1 " + node.getSubjectVar());
		statementPatterns.add(node);
		if(node.getSubjectVar().isAnonymous())
			subjectURIstring.append("(<").append(node.getSubjectVar().getValue()).append(">)");
		else
			subjectVariables.add(node.getSubjectVar().getName());
		if(node.getPredicateVar().isAnonymous())
			predicateURIstring.append("(<").append(node.getPredicateVar().getValue()).append(">)");
		else
			predicateVariables.add(node.getPredicateVar().getName());
		if(node.getObjectVar().isAnonymous())
			objectURIstring.append("(<").append(node.getObjectVar().getValue()).append(">)");
		else
			objectVariables.add(node.getObjectVar().getName());
//		if(!node.getSubjectVar().isAnonymous() && node.getPredicateVar().isAnonymous() && node.getObjectVar().isAnonymous()) //this means that the statement pattern is {var notVar notVar}
//		{
//			if(node.getPredicateVar().getValue().equals(RDF.TYPE)) // this means that it is a type triple
//				typeHash.put(node.getSubjectVar().getValue() + "", node.getObjectVar().getValue() + "");
//			else if (node.getPredicateVar().getValue().equals(RDFS.SUBPROPERTYOF))
//				subpropHash.put(node.getSubjectVar().getValue() + "", node.getObjectVar().getValue() + "");
//		}
//		try {
//			// super.meet(node);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	@Override
	public void meet(ProjectionElem node) {
		//System.out.println("here2");
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

	/*public void meet(ExtensionElem node) {
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
			System.err.println("Constant Hash is " + constantHash + source + collector2.value);
			constantHash.put(source, collector2.value);
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// projections.add(node);
	}*/

	@Override
	public void meet(Coalesce node) {
		//System.out.println("here3");
		// System.out.println("Coalesce is  " + node.getArguments().get(0) );
		// System.out.println("Parent " + node.getParentNode());
		try {
			VarNameCollector collector = new VarNameCollector();
			node.visit(collector);
			//System.out.println("Yoo hoo" + collector.getVarNames());

		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void meet(LocalName constant) {
		//System.out.println("here4");
		// System.out.println("Constants is " + constant);
	}

	// @Override
	public void meet2(ExtensionElem node) {
		//System.out.println("Extension Element is  " + node);
		//System.out.println(node.getName());
		//System.out.println(node.getExpr());
	}

	public List<StatementPattern> getPatterns() {
		return this.statementPatterns;
	}
	
	public StringBuffer getSubjectURIstring() {
		return subjectURIstring;
	}

	public StringBuffer getPredicateURIstring() {
		return predicateURIstring;
	}

	public Set<String> getSubjectVariables() {
		return subjectVariables;
	}

	public Set<String> getPredicateVariables() {
		return predicateVariables;
	}

	public Set<String> getObjectVariables() {
		return objectVariables;
	}

	public StringBuffer getObjectURIstring() {
		return objectURIstring;
	}
}