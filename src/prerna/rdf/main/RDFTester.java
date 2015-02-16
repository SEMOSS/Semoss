/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.rdf.main;

import info.aduna.iteration.CloseableIteration;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;



/**
 */
public class RDFTester {
	
	static final Logger logger = LogManager.getLogger(RDFTester.class.getName());
	
	SailConnection sc = null;
	Sail sail = null;
	ValueFactory vf = null;
	
	/**
	 * Method openGraph.
	 */
	public void openGraph() throws Exception {
		//svc = new EmbeddedGraphDatabase("NeoRDF41.db");
		//ng = new Neo4jGraph(svc);
		//ng.startTransaction();
		//sail = (Sail) new GraphSail(ng);
		sail = new ForwardChainingRDFSInferencer(new MemoryStore());
		sail.initialize();
		sc = sail.getConnection();
		vf = sail.getValueFactory();
		//return g;
	}
	
	/**
	 * Method closeGraph.
	 */
	public void closeGraph() throws Exception
	{
		//ng.stopTransaction(Conclusion.SUCCESS);
		sail.shutDown();
		//ng.shutdown();
	}
	
	/**
	 * Method createRDFData.
	 */
	public void createRDFData() throws Exception
	{
		// pk.com#knows subclassof pk.com#connected
		// pk.com#pknows typeof pk.com#connection
		// pk.com#connected typeof pk.com#connection
		
		URI knows = new URIImpl("http://pk.com#knows");
		URI connected = new URIImpl("http://pk.com#connected");
		URI knows2 = new URIImpl("http://pk.com#knows2");
		
		sc.addStatement(knows, RDFS.SUBPROPERTYOF, connected);
		sc.addStatement(knows2, RDFS.SUBPROPERTYOF, knows);

		// other statements
		sc.addStatement(vf.createURI("http://pk.com#1"), knows2, vf.createURI("http://pk.com#3"));
		sc.addStatement(knows2,vf.createURI("http://pk.com#has"), vf.createURI("http://pk.com#properties"));
		sc.addStatement(vf.createURI("http://pk.com#1"), vf.createURI("http://pk.com#name"), vf.createLiteral("PK"), vf.createURI("http://pk.com"));
		sc.addStatement(vf.createURI("http://pk.com#3"), vf.createURI("http://pk.com#name"), vf.createLiteral("PK2"), vf.createURI("http://pk.com"));
		sc.addStatement(vf.createURI("http://pk.com#1"), vf.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf"), vf.createURI("http://pk.com#entity"), vf.createURI("http://pk.com"));

	}
	
	/**
	 * Method createSubClassTest.
	 */
	public void createSubClassTest() throws Exception
	{
		Resource beijing = new URIImpl("http://example.org/things/Beijing");
		Resource city = new URIImpl("http://example.org/terms/city");
		Resource place = new URIImpl("http://example.org/terms/place");
		
		Resource beijing2 = new URIImpl("http://example.org/things/Beijing");

		//Sail reasoner = new ForwardChainingRDFSInferencer(new GraphSail(new TinkerGraph()));
		//reasoner.initialize();

		try {
		    //SailConnection c = reasoner.getConnection();
		    try {
		        sc.addStatement(city, RDFS.SUBCLASSOF, place);
		        sc.addStatement(beijing, RDF.TYPE, city);
		        sc.commit();

		        CloseableIteration<? extends Statement, SailException> i
		                = sc.getStatements(beijing2, null, null, true);
		        try {
		            while (i.hasNext()) {
		            	logger.debug("statement " + i.next());
		            }
		        } finally {
		            i.close();
		        }
		    } finally {
		        //sc.close();
		    }
		} finally {
		    //reasoner.shutDown();
		}
	}

/*	public void createBlankNodeTest() throws Exception
	{
		Resource myBNode = vf.createBNode("http://pk.com/#1BNode");
		Resource curNode = vf.createURI("http://pk.com/#1");
		
		
		
		//Sail reasoner = new ForwardChainingRDFSInferencer(new GraphSail(new TinkerGraph()));
		//reasoner.initialize();

		try {
		    //SailConnection c = reasoner.getConnection();
		    try {
		    	// add all the stuff related to BNode
		    	sc.addStatement(vf.createURI("http://pk.com/#1"), vf.createURI(""), vf.createURI(""));
		        sc.addStatement(city, RDFS.SUBCLASSOF, place);
		        sc.addStatement(beijing, RDF.TYPE, city);
		        sc.commit();

		        CloseableIteration<? extends Statement, SailException> i
		                = sc.getStatements(beijing2, null, null, true);
		        try {
		            while (i.hasNext()) {
		                logger.debug("statement " + i.next());
		            }
		        } finally {
		            i.close();
		        }
		    } finally {
		        //sc.close();
		    }
		} finally {
		    //reasoner.shutDown();
		}
	}*/

	
	/**
 * Method doSPARQL.
 */
public void doSPARQL() throws Exception
	{
		SPARQLParser parser = new SPARQLParser();
		CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;
		String queryString = "SELECT ?x ?y WHERE { ?x <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?y ." +
				" <http://pk.com#1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?y." +
				" }";
		ParsedQuery query = parser.parseQuery(queryString, "http://pk.com");

		logger.debug("\nSPARQL: " + queryString);
		sparqlResults = sc.evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
		while (sparqlResults.hasNext()) {
			logger.debug(sparqlResults.next());
		}
		
	}


	/**
	 * Method readDataAsRDF.
	 */
	public void readDataAsRDF() throws Exception
	{
		CloseableIteration results;
		/*logger.debug("get statements: ?s ?p ?o ?g");
		results = sc.getStatements(null, null, null, false);
		while(results.hasNext()) {
		    logger.debug(results.next());
		}*/

		logger.debug("\nget statements: http://pk.com#knows ?p ?o ?g");
		results = sc.getStatements(vf.createURI("http://pk.com#knows2"), null, null, true);
		while(results.hasNext()) {
			logger.debug(results.next());
		}		

		logger.debug("\nget statements: http://pk.com#1 ?p ?o ?g");
		results = sc.getStatements(vf.createURI("http://pk.com#1"),null, null, false);
		while(results.hasNext()) {
			logger.debug(results.next());
		}		

		logger.debug("\nget statements: http://pk.com#connection ?p ?o ?g");
		results = sc.getStatements(null, vf.createURI("http://pk.com#connected"), null, false);
		while(results.hasNext()) {
			logger.debug(results.next());
		}		

		logger.debug("\nget statements: http://pk.com#entity ?p ?o ?g");
		results = sc.getStatements(vf.createURI("http://pk.com#entity"),null, null, false);
		while(results.hasNext()) {
			logger.debug(results.next());
		}		

		
		
	}
	

	// load the RDF
	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String [] args) throws Exception
	{
		try
		{
			RDFTester tester = new RDFTester();
			tester.openGraph();
			//tester.createRDFData();
			//tester.createGraphData();
			tester.createSubClassTest();
			//tester.readDataAsRDF();
			//tester.doSPARQL();
			//tester.readDataAsGraph();
			tester.closeGraph();
			
		}catch(Exception ignored)
		{
			System.err.println("Exception " + ignored);
			ignored.printStackTrace();
		} 
		
		
	}
	
	
}
