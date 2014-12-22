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

import java.util.Vector;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.GraphPattern;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;

import prerna.rdf.engine.api.IEngine;

public class QueryBuilderHelper {
	ValueFactory vf = null;
	RepositoryConnection rc = null;
	SailRepositoryConnection sc = null;

	// this the primary class that will be turned into a REST service for query builder
	// The query builder typically needs to have a way to do a couple of things
	// get all the Classes / concepts
	// get all the properties for a given class
	// get all the relation for a given class
	// Get all instances for a given class
	// Get all instance for a given property
	// this is impending on selecting a particular instance to get to those properties
	// Get all instances for a given relation
	// this is again impending on selecting a instance of the class
	
	// The type is always shown as tree node with the instance of this type right below it
	// the relation is also shown as a node in the tree
	// each node and leaf on the tree has a check box
	// as you expand a tree, it will automatically check the box
	// When a class type node is selected it creates a triple <outputclass> <typeof> <selectednode>
	// When a property on the class type node it adds <outputclass> <propertyname> <outputproperty>
	// When an instance is selected it adds <bind> <outputclass> <instancename>
	// When a relationship is selected <outputclass> <selectedrelation> <outputclass2>, 
	// and the other entity is selected <bind> <outputclass2> <instancename2>
	// when the property is selected same thing
	
	public void setEngine(IEngine engine)
	{
		// will set the engine
	}
	
	
	
	public String[][] getClasses()
	{
		// returns all the classes
		// basically anything which is an instance of concept will be returned here
		// the array is 2 dimensional because it will have the concatenated class name and the value
		// key is the name for display and value being the URI
		// from a JSON standpoint, the second dimension would be hidden		
		// SPARQL - <All the things> <subclassof> <Concept>
		
		return null;
	}
	
	public String[][] getClassInstance(String classType)
	{
		// this is passing in the value from the getClasses
		// which once again gets a 2 dimensional array
		// key being the name of instance and the value being URI of the instance
		// composes a SPARQL query using the grammar
		// with <all the things> <typeof> <classType>
		
		return null;
	}
	
	// gets a particular relation
	public String[][] getRelationInstance(String fromType, String fromInstanceName, String relationName, String toType)
	{
		// gets the relations for the given parameters
		// <fromtypename> <relationname> <totypename>
		// <fromtypename> <typeof> <fromtype>
		// <totypename> <typeof> <totype>
		// <Bind> <fromtypename> <frominstancename> 
		
		return null;
	}
	
	public String[][] getRelations(String classType)
	{
		// given a typical class - this gives all the relationships
		// 2 dimensional - key is the name of the relation and the URI
		// SPARQL
		// <class type> <has metarelation> <returnrelation> 
		// <classtype> <subclassof> <concept>
		
		return null;
	}
	
	public String[][] getOtherEntity(String classType, String relationName)
	{
		// SPARQL
		// <classType> <relationName> <otherentity>
		// <classType> <subclassof> <concept>
		// <otherentity> <subclassof> <concept>
		
		return null;
	}
	
	public String[][] getProperties(String classType)
	{
		// given a typical class - this gives all the relationships
		// 2 dimensional - key is the name of the relation and the URI
		// SPARQL
		// <class type> <has metaproperty> <property>
		// <class type> <subclassof> <concept>
		// or may be plain bind may work too, need to check it out
		
		
		return null;
	}
	
	// get the property instances for a type
	public String [][] getPropertiesForInstance(String classType, String instanceName)
	{
		// it has to first find all the properties for this class type
		// and get each of the instances
		// SPARQL
		// <classtype> <metaproperty> <property>
		// <instanceName> <property> <output>
		// that is it really
		return null;
	}

	public void testQueryGen() {
		
		try {
			Var systemName = new Var("x");
			Var typeRelation = new Var("y", vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));// , factory.createURI(...));
			//URI y1 = vf
				//	.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
			Var systemType = new Var("z", vf.createURI("http://semoss.org/ontologies/Concept/System"));
			//z = (Var) vf.createURI("http://semoss.org/ontologies/Concept/System");
			Var p = new Var("p");
			Var w = new Var("w");
			Var t = new Var("t");

			GraphPattern gp = new GraphPattern();
			//gp.addConstraint(new Compare(p, new ValueConstant(vf.createURI("http://semoss.org/ontologies/Concept/System"))));
			//gp.addConstraint(new Compare(w, new ValueConstant(vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))));
			
			// Filter
			//gp.addConstraint(new Like(systemName,"*",false));
			// adding bindings
			BindingSetAssignment bsa = new BindingSetAssignment();
			SPARQLQueryBindingSet bs2 = new SPARQLQueryBindingSet();
			Vector <BindingSet> vbs = new Vector<BindingSet>();
			vbs.addElement(bs2);
			bs2.addBinding("p", vf.createURI("http://semoss.org/ontologies/Concept/System")); // binding 1
			bs2.addBinding("t", vf.createURI("http://semoss.org/ontologies/Concept/System")); // binding 2
			bsa.setBindingSets(vbs);
			gp.addRequiredTE(bsa);
					
			
			// adding coalesce
			Coalesce c = new Coalesce();
			c.addArgument(t);
			c.addArgument(new ValueConstant(vf.createURI("http://semoss.org")));
			
			ExtensionElem cee = new ExtensionElem(c,"t");
			Extension ce = new Extension(new GraphPattern().buildTupleExpr());
			ce.addElement(cee);
			//gp.addRequiredTE(ce);

			// adding triples
			gp.addRequiredSP(systemName, w, p);

			System.out.println(gp.buildTupleExpr());

			Projection proj = new Projection();
			ProjectionElemList list = new ProjectionElemList();
			list.addElements(new ProjectionElem("x"), new ProjectionElem("t"));
			
			//gp.addRequiredTE(proj);
			//System.out.println(gp.buildTupleExpr());
			
			//gp.addRequiredTE()

			/*gp.addRequiredTE(new Projection(gp.buildTupleExpr(),
					new ProjectionElemList(new ProjectionElem("x")
					,new ProjectionElem("t"))));
	*/
			//System.out.println(gp.buildTupleExpr());
			//
			
			TupleExpr query2 = new Projection(gp.buildTupleExpr(),
					new ProjectionElemList(new ProjectionElem("x")
					,new ProjectionElem("t")
					// new ProjectionElem("y")
					));
			
			
			//query2.addRequiredTE(ce);
			
			
			//gp.addRequiredTE(bindE);
			
			
			//gp.addRequiredTE(new )
			
			// gp.addRequiredSP(x, p, w);
			// gp.addRequiredSP(x, y, w);
			// gp.addConstraint(new Compare(x, new
			// ValueConstant(vf.createURI("http://semoss.org")));


			ParsedTupleQuery query3 = new ParsedTupleQuery(query2);
			SailTupleQuery q = new MyTupleQuery(query3,
					(SailRepositoryConnection) rc);
			
			System.out.println("\nSPARQL: " + query3);

			TupleQueryResult sparqlResults = q.evaluate();
			//tq.setIncludeInferred(true /* includeInferred */);
			//TupleQueryResult sparqlResults = tq.evaluate();

			System.out.println("Output is " );
			while (sparqlResults.hasNext()) {
				BindingSet bs = sparqlResults.next();
				System.out.println("Predicate >>> " + bs.getBinding("x") + "  >>> " + bs.getBinding("t"));
			}
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
