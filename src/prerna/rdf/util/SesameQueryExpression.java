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

import java.util.Hashtable;
import java.util.Vector;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Like;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.sparql.GraphPattern;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;

public class SesameQueryExpression {
	
	GraphPattern finalExpr = new GraphPattern();
	ValueFactory vf = new ValueFactoryImpl();
	int classCount = 0;
	int instanceCount = 0;
	int propCount = 0;
	int relCount = 0;
	int typeCount = 0;
	Vector <BindingSet> vbs = new Vector<BindingSet>();
	Vector <Extension> cList = new Vector<Extension>();
	Vector <ValueExpr> constraintList = new Vector<ValueExpr>();
	Projection proj = new Projection(new GraphPattern().buildTupleExpr());
	
	// this wont work when I actually have multiple things going into the same system
	Hashtable <String, String> typeInstanceHash = new Hashtable<String, String>();
	Hashtable <String, Var> varHash = new Hashtable<String, Var>();
	
	public static final String clazz = "CLASS";
	public static final String instance = "INST";
	public static final String prop = "PROP";
	public static final String rel = "RELATION";
	
	// add a required triple with three strings
	public void addTriple(String subjectName, String predicateName, String objectName, String subjectVal, String predicateVal, Object objectVal, boolean uri)
	{
		Var vSubject = new Var(subjectName);
		if(subjectVal != null)
			new Var(subjectName, vf.createURI(subjectVal));
		Var vPredicate = new Var(predicateName);
		if(predicateVal != null)
			new Var(predicateName, vf.createURI(predicateVal));
		Var vObject = new Var(objectName);
		if(objectVal != null)
		{
			// understand the object and create the value accordingly
			// logic for string
			if(uri)
				vObject = new Var(objectName, vf.createURI(objectVal+""));
			else if(objectVal instanceof String)
				vObject = new Var(objectName, vf.createLiteral((String)objectVal));
			else if(objectVal instanceof Double)
				vObject = new Var(objectName, vf.createLiteral(((Double)objectVal).doubleValue()));
			else if(objectVal instanceof Integer)
				vObject = new Var(objectName, vf.createLiteral(((Double)objectVal).intValue()));
			else // defaulted to string
				vObject = new Var(objectName, vf.createLiteral(objectVal + ""));
			//else if(objectVal instanceof Date)
			//	vObject = new Var(objectName, vf.createLiteral((Date)objectVal));
		}			
		varHash.put(subjectName, vSubject);
		varHash.put(predicateName, vPredicate);
		varHash.put(objectName, vObject);
		finalExpr.addRequiredSP(vSubject, vPredicate, vObject);
	}
	
	public String addClassTypeTriple(String classType)
	{
		if(!typeInstanceHash.containsKey(classType))
		{	
			String outClass = clazz + classCount;
			classCount++;
			typeInstanceHash.put(classType, outClass);
			addTriple(outClass, "type", clazz + classCount, null, RDF.TYPE+"", classType, true);
			classCount++;
		}
		return typeInstanceHash.get(classType);
	}
	
	public void addTypeProjection(String classType)
	{
		// projection name
		String projName = addClassTypeTriple(classType);
		ProjectionElemList list = new ProjectionElemList();
		if(proj.getProjectionElemList() != null)
			list = proj.getProjectionElemList();
		list.addElements(new ProjectionElem(projName));	
		proj.setProjectionElemList(list);
		// we will add this in the end
		//finalExpr.addRequiredTE(proj);
	}
	
	// this is what happens, when we select a particular instance
	public String addInstanceTriple(String classType, String constantURI)
	{
		// add an instance is a type of class and then
		// bind that instance name to this constant name
		if(!typeInstanceHash.containsKey(constantURI))
		{
			String className = addClassTypeTriple(classType);
			String outInstance = instance + instanceCount;
			instanceCount++;
			typeInstanceHash.put(constantURI, outInstance);
			addTriple(outInstance, "type", className, null, RDF.TYPE+"", classType, true);
			// do the binding here
			addURIBinding(outInstance, constantURI);
		}
		return typeInstanceHash.get(constantURI);
	}
	
	public void addURIBinding(String varName, String constantURI)
	{
		//Vector <BindingSet> vbs = new Vector<BindingSet>();
		//vbs.addElement(bs2);
		SPARQLQueryBindingSet bs2 = new SPARQLQueryBindingSet();
		
		// need to the other types of bindings
		bs2.addBinding(varName, vf.createURI(constantURI)); // binding 1
		
		vbs.add(bs2);
	}
	
	public String addPropertyTriple(String classType, String propertyType)
	{
		// Rethink this
		// add the classtype if it not already there
		// classInstance typeof classtype
		// propertyInstance typeof propertytype
		// classInstance propertyType propertyInstance
		if(!typeInstanceHash.containsKey(classType+propertyType))
		{
			// classInstance typeof classtype
			String classInstanceName = addClassTypeTriple(classType);

			// propertyInstance typeof propertytype
			// now add the propertytype to this
			String outputInstanceProp = prop + propCount;
			propCount++;
			String outputProp = prop + propCount;
			propCount++;
			typeInstanceHash.put(classType+propertyType, outputInstanceProp);
			
			String outputProjection = prop+propCount;
			propCount++;
			
			// add the property type first
			addTriple(outputInstanceProp, "type", outputProp, null, RDF.TYPE+"", propertyType, true);
		
			// classInstance propertyType propertyInstance
			addTriple(classInstanceName, outputInstanceProp, outputProjection, null, propertyType, null, true);
		}
		return typeInstanceHash.get(classType + propertyType);
	}
	
	// relationship is added only when the other end is clicked
	public String addRelationTriple(String startType, String relationType, String endType)
	{
		if(!typeInstanceHash.containsKey(startType+relationType+endType))
		{
			String className = addClassTypeTriple(startType);
			String className2 = addClassTypeTriple(endType);
			String relationName = rel + relCount;
			relCount++;
			String relationVarName = rel + relCount;
			relCount++;
			//add the relationtriple first
			addTriple(relationName, "type", relationVarName, null, RDF.TYPE + "", relationType, true);
			
			// add the class relationtriple next
			addTriple(className, relationName,className2, null, null, null, false);			
			typeInstanceHash.put(startType + relationType + endType, relationName);
		}
		return typeInstanceHash.get(startType + relationType + endType);
	}
	
	public void addClassInstanceProjection(String classType, String constantURI)
	{
		String instanceName = addInstanceTriple(classType, constantURI);
		ProjectionElemList list = new ProjectionElemList();
		if(proj.getProjectionElemList() != null)
			list = proj.getProjectionElemList();
		list.addElements(new ProjectionElem(instanceName));	
		proj.setProjectionElemList(list);
	}

	public void addPropertyInstanceProjection(String classType, String propertyType, String constantURI)
	{
		String instanceName = addPropertyTriple(classType, constantURI);
		ProjectionElemList list = new ProjectionElemList();
		if(proj.getProjectionElemList() != null)
			list = proj.getProjectionElemList();
		list.addElements(new ProjectionElem(instanceName));	
		proj.setProjectionElemList(list);
	}

	public void addRelationInstanceProjection(String fromType, String relationType, String toType, String constantURI)
	{
		String instanceName = addRelationTriple(fromType, relationType, toType); 
		ProjectionElemList list = new ProjectionElemList();
		if(proj.getProjectionElemList() != null)
			list = proj.getProjectionElemList();
		list.addElements(new ProjectionElem(instanceName));	
		proj.setProjectionElemList(list);
	}

	// BIND VALUES
	// classtype
	public void addClassTypeBinding(String classType, String constantURI)
	{
		String className = addClassTypeTriple(classType);
		addURIBinding(className, constantURI);
	}
	
	// relationtype
	public void addRelationTypeBinding(String fromType, String relationType, String toType, String constantURI)
	{
		String relationName = addRelationTriple(fromType, relationType, toType);
		addURIBinding(relationName, constantURI);
	}
		
	// setting default values
	// need some way to coalesce here
	public void setDefault(String classType, String defaultValue)
	{
		String className = addClassTypeTriple(classType);
		Coalesce c = new Coalesce();
		
		c.addArgument(varHash.get(className));
		
		// TODO need to write the logic for other types of objects
		c.addArgument(new ValueConstant(vf.createURI(defaultValue)));
		
		ExtensionElem cee = new ExtensionElem(c,className);
		Extension ce = new Extension(new GraphPattern().buildTupleExpr());
		ce.addElement(cee);
		
		cList.addElement(ce);
	}
	
	// Filter Values
	public void addClassFilter(String type, String value)
	{
		String className = addClassTypeTriple(type);
		ValueExpr expr = new Like(varHash.get(className), value, false);
		constraintList.add(expr);		
	}
	
	// add derived variables
	public void addMathVariable(String operand1, String operand2, String expression)
	{
		// the operator is typically 
		// a+b
		
		
		ValueConstant arg1 = new ValueConstant(vf.createLiteral(2.0));
		ValueConstant arg2 = new ValueConstant(vf.createLiteral(4.0));
		MathExpr mathExpr = new MathExpr(arg1, arg2, MathExpr.MathOp.PLUS);

		
		// adding coalesce
		Coalesce c = new Coalesce();
		//c.addArgument(p);
		//c.addArgument(new ValueConstant(vf.createURI("http://semoss.org")));
		c.addArgument(mathExpr);

	}
	
	// need to do this

	public GraphPattern buildExpr()
	{
		GraphPattern gp = new GraphPattern();
		gp.addRequiredTE(proj);
		
		// adding the constraints
		// these are things like filters etc. 
		for(int constraintIndex = 0;constraintIndex < constraintList.size();constraintIndex++)
			gp.addConstraint(constraintList.elementAt(constraintIndex));
		
		// add the coalesce stuff next
		
		gp.addRequiredTE(finalExpr.buildTupleExpr());
		return gp;
	}
	
	public static void main(String [] args)
	{
		// tests 
		
		SesameQueryExpression expr = new SesameQueryExpression();
		expr.addTypeProjection("http://semoss.org/concept/system");
		expr.addRelationTriple("http://semoss.org/concept/system", "http://semoss.org/relation/provide", "http://semoss.org/concept/do");
		expr.addRelationTriple("http://semoss.org/concept/service", "http://semoss.org/relation/exposes", "http://semoss.org/concept/do");
		System.out.println(expr.buildExpr().buildTupleExpr());
		
	}
	
	
	
}
