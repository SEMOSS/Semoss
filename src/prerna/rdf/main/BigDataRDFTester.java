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
package prerna.rdf.main;

import info.aduna.iteration.CloseableIteration;

import java.io.FileInputStream;
import java.net.URLEncoder;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;

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
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.stringtemplate.v4.ST;

import prerna.om.DBCMVertex;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.graph.Forest;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;

/**
 */
public class BigDataRDFTester {
	
	static final Logger logger = LogManager.getLogger(BigDataRDFTester.class.getName());

	BigdataSail bdSail = null;
	Properties bdProp = null;
	Properties rdfMap = null;
	SailRepositoryConnection rc = null;
	SailConnection sc = null;
	ValueFactory vf = null;

	/**
	 * Method openGraph.
	 */
	public void openGraph() throws Exception {

		bdSail = new BigdataSail(bdProp);
		// BigdataSail.Options.TRUTH_MAINTENANCE = "true";
		BigdataSailRepository repo = new BigdataSailRepository(bdSail);
		repo.initialize();
		rc = repo.getConnection();

		//bdSail.
		// new ForwardChainingRDFSInferencer(bdSail);
		InferenceEngine ie = bdSail.getInferenceEngine();
		logger.info("ie forward chaining " + ie);
		//logger.info("Truth " + bdSail.isTruthMaintenance());
		logger.info("quad " + bdSail.isQuads());
		
		// logger.info("ie forward chaining " + ie);

		// SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		sc = (rc).getSailConnection();
		String propFile = workingDir + "/DBCM_RDF_Map.prop";
		loadRDFMap(propFile);

		// sail = new ForwardChainingRDFSInferencer(new GraphSail(ng));
		// sail.initialize();
		// sc = sail.getConnection();
		vf = bdSail.getValueFactory();
		// return g;
	}

	/**
	 * Method loadProperties.
	 * @param fileName String
	 */
	public void loadProperties(String fileName) throws Exception {
		bdProp = new Properties();
		bdProp.load(new FileInputStream(fileName));
		logger.debug("Properties >>>>>>>>" + bdProp);

	}
	
	/**
	 * Method loadRDFMap.
	 * @param fileName String
	 */
	public void loadRDFMap(String fileName) throws Exception
	{
		rdfMap = new Properties();
		rdfMap.load(new FileInputStream(fileName));
	}

	/**
	 * Method closeGraph.
	 */
	public void closeGraph() throws Exception {
		// ng.stopTransaction(Conclusion.SUCCESS);
		bdSail.shutDown();
		// ng.shutdown();
	}

	/**
	 * Method createRDFData.
	 */
	public void createRDFData() throws Exception {
		// pk.com#knows subclassof pk.com#connected
		// pk.com#pknows typeof pk.com#connection
		// pk.com#connected typeof pk.com#connection

		URI knows = new URIImpl("http://pk.com#knows");
		URI connected = new URIImpl("http://pk.com#connected");
		URI knows2 = new URIImpl("http://pk.com#knows2");

		sc.addStatement(knows, RDFS.SUBPROPERTYOF, connected);
		sc.addStatement(knows2, RDFS.SUBPROPERTYOF, knows);

		// other statements
		sc.addStatement(vf.createURI("http://pk.com#1"), knows2,
				vf.createURI("http://pk.com#3"));
		sc.addStatement(knows2, vf.createURI("http://pk.com#has"),
				vf.createURI("http://pk.com#properties"));
		sc.addStatement(vf.createURI("http://pk.com#1"),
				vf.createURI("http://pk.com#name"), vf.createLiteral("PK"),
				vf.createURI("http://pk.com"));
		sc.addStatement(vf.createURI("http://pk.com#3"),
				vf.createURI("http://pk.com#name"), vf.createLiteral("PK2"),
				vf.createURI("http://pk.com"));
		sc.addStatement(
				vf.createURI("http://pk.com#1"),
				vf.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf"),
				vf.createURI("http://pk.com#entity"),
				vf.createURI("http://pk.com"));

	}

	/**
	 * Method createSubClassTest.
	 */
	public void createSubClassTest() throws Exception {
		Resource beijing = new URIImpl("http://example.org/things/Beijing");
		Resource city = new URIImpl("http://example.org/terms/city");
		Resource place = new URIImpl("http://example.org/terms/place");

		Resource beijing2 = new URIImpl("http://example.org/things/Beijing");

		// Sail reasoner = new ForwardChainingRDFSInferencer(new GraphSail(new TinkerGraph()));
		// reasoner.initialize();

		try {
			// SailConnection c = reasoner.getConnection();
			try {
				sc.addStatement(city, RDFS.SUBCLASSOF, place);
				sc.addStatement(beijing, RDF.TYPE, city);

				InferenceEngine ie = bdSail.getInferenceEngine();
				ie.computeClosure(null);

				sc.commit();

				CloseableIteration<? extends Statement, SailException> i = sc
						.getStatements(beijing, null, null, true);
				try {
					while (i.hasNext()) {
						logger.info("statement " + i.next());
					}
				} finally {
					i.close();
				}
			} finally {
				// sc.close();
			}
		} finally {
			// reasoner.shutDown();
		}
	}

	/*
	 * public void createBlankNodeTest() throws Exception { Resource myBNode = vf.createBNode("http://pk.com/#1BNode");
	 * Resource curNode = vf.createURI("http://pk.com/#1");
	 * 
	 * 
	 * 
	 * //Sail reasoner = new ForwardChainingRDFSInferencer(new GraphSail(new TinkerGraph())); //reasoner.initialize();
	 * 
	 * try { //SailConnection c = reasoner.getConnection(); try { // add all the stuff related to BNode
	 * sc.addStatement(vf.createURI("http://pk.com/#1"), vf.createURI(""), vf.createURI("")); sc.addStatement(city,
	 * RDFS.SUBCLASSOF, place); sc.addStatement(beijing, RDF.TYPE, city); sc.commit();
	 * 
	 * CloseableIteration<? extends Statement, SailException> i = sc.getStatements(beijing2, null, null, true); try {
	 * while (i.hasNext()) { logger.info("statement " + i.next()); } } finally { i.close(); } } finally {
	 * //sc.close(); } } finally { //reasoner.shutDown(); } }
	 */

	/**
	 * Method doSPARQL.
	 */
	public void doSPARQL() throws Exception {

		SPARQLParser parser = new SPARQLParser();
		// CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;
		// String queryString =
		// "SELECT ?x ?y WHERE {?x <http://semoss.org/ontologies/Relation/Supports/Immunization_Immunizations> ?y}";
		// // +
		// " <http://pk.com#1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?y." +
		// " }";
		// String queryString =
		// "SELECT ?x WHERE {<http://semoss.org/ontologies/Concept/Capability/Immunization>  <"+RDF.TYPE+"> ?x}";
		// // +
		// String queryString =
		// "SELECT ?x WHERE {<http://semoss.org/ontologies/Relation/Supports/Immunization_Immunizations> <http://semoss.org/ontologies/Relation> ?x}";
		String queryString = "SELECT * WHERE {?y <http://semoss.org/ontologies/Relation/Contains> ?x}";
		// SparqlBuilder
		// String queryString2 = "SELECT * FROM {Cap}  " + "<http://semoss.org/ontologies/Relation/Supports>" +
		// "  {BP}";
		String queryString2 = "SELECT x, y FROM {x} p {y}";

		TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		// GraphQuery gq = rc.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
		// GraphQuery gq = rc.prepareGraphQuery(QueryLanguage.SERQL, queryString2);

		// Graph g = null;

		// GraphQueryResult gqrs = gq.evaluate();
		// gqrs.
		// tq.e
		// ParsedQuery query = parser.parseQuery(queryString, "http://pk.com");

		logger.debug("\nSPARQL: " + queryString);
		tq.setIncludeInferred(true /* includeInferred */);
		// gq.evaluate();
		// sparqlResults = sc.evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
		TupleQueryResult sparqlResults = tq.evaluate();
		while (sparqlResults.hasNext()) {
			logger.debug("Inside");
			BindingSet bs = sparqlResults.next();
			logger.debug(bs);
		}
		/*
		 * Iterator uriIt = tq.getDataset().getDefaultGraphs().iterator(); while(uriIt.hasNext()) {
		 * logger.debug("The URI is " + uriIt.next()); }
		 */

	}
	
	/**
	 * Method doRemoveSelect.
	 * @param queryString String
	 * @param names String[]
	 */
	public void doRemoveSelect(String queryString, String [] names) throws Exception
	{
		logger.debug("\nSPARQL: " + queryString);
		TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tq.setIncludeInferred(true /* includeInferred */);
		// gq.evaluate();
		// sparqlResults = sc.evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
		SPARQLResultsJSONWriter w = new SPARQLResultsJSONWriter(System.out);
		TupleQueryResult sparqlResults = 
		tq.evaluate();
		//sparqlResults.
		
		names = null;
		
		logger.info("Size is " + tq.getBindings().size());
		
		while (sparqlResults.hasNext()) {
			logger.debug(">>>>> ");
			BindingSet bs = sparqlResults.next();
			logger.debug(" Binding set binding names " + bs.getBindingNames().size());
			//String name2 = bs.getBindingNames().iterator().next();
			//logger.debug(" name value is " + name2);
			//logger.debug(" Value is " + bs.getValue(name2) + "");
			if(names == null)
			{
				names = new String[bs.getBindingNames().size()];
				Iterator nameIt = bs.getBindingNames().iterator();
				for(int nameIndex = 0;nameIndex < names.length; nameIndex++)
				{
					String name = nameIt.next() + "";
					logger.debug("Name is  " + name);
					names[nameIndex] = name;
					
				}

			}
			for(int nameIndex = 0;nameIndex < names.length;nameIndex++) {
				if(nameIndex < names.length - 1) {
					logger.debug("\t" + bs.getValue(names[nameIndex]));
				} else {
					logger.debug(bs.getValue(names[nameIndex]));
				}
			}
		}
		
	}

	/**
	 * Method doRemoteSPARQL.
	 * @param queryString String
	
	 * @return GraphQueryResult */
	public GraphQueryResult doRemoteSPARQL(String queryString) throws Exception {
		// String queryString = "SELECT * WHERE {?y <http://semoss.org/ontologies/Relation/Contains> ?x}";
		// String queryString = "CONSTRUCT {?fromObjectUri ?relationUri ?o .}WHERE" +
		// " { {?fromObjectUri ?relationUri ?o .} }";
		// String queryString = "CONSTRUCT {?x <" + RDFS.SUBCLASSOF +
		// "> <http://semoss.org/ontologies/Concept/Lifecycle> .} WHERE" + " { {?x <" + RDFS.SUBCLASSOF +
		// "> ?y .} }";

		 //String queryString = "SELECT ?y WHERE" + " {?y <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/BusinessProcess>}";
		 
		 
		 /* Final Working Copy
		  * String queryString = "CONSTRUCT {?x ?y ?z.} WHERE" + " { {?z <" + RDF.TYPE + "> " +
		 		"<http://semoss.org/ontologies/Concept/DataObject>;} " +
		 		"{?y <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Payload> ;}" + 
		 		" {?x ?y ?z .}}";
		 */
		
		//String queryString = "CONSTRUCT {?x ?y ?z .} WHERE "
		//		+ "{ {?x <"+ RDFS.TYPE + "> <http://semoss.org/ontologies/Concept/BusinessProcess> .}}";


		 //TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		
		 GraphQuery sagq = rc.prepareGraphQuery(QueryLanguage.SPARQL,
				queryString);
		//sagq.setBinding("o", vf.createURI("http://semoss.org/ontologies/Concept/Lifecycle"));
		//sagq.setBinding("relationUri", RDFS.SUBCLASSOF);

		GraphQueryResult res = sagq.evaluate();
		// TupleQueryResult res = tq.evaluate();

		return res;
		// logger.debug("result is " + res.);
		/*while (res.hasNext()) {
			//BindingSet bs = res.next();
			Statement st = res.next();
			logger.debug(st.getSubject() + "<>" + st.getPredicate()
					+ "<>" + st.getObject());
			//logger.info("Binding is " + bs);
		}*/
	}
	
	/**
	 * Method convertSailtoGraph.
	 * @param res GraphQueryResult
	
	 * @return Forest */
	public Forest convertSailtoGraph(GraphQueryResult res) throws Exception
	{
		DelegateForest df = new DelegateForest(); 
		Hashtable vertStore = new Hashtable();
		while(res.hasNext())
		{
			//logger.info("Iterating ...");
			Statement st = res.next();
			
			String predicate = st.getPredicate()+"";
			logger.debug(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			if(predicate.contains("Relation") && !rdfMap.contains(predicate) ) // need to change this to starts with relation
			{
				//logger.debug("Adding" + st.getPredicate());
				if(vertStore.get(st.getSubject()) == null)
				{
					DBCMVertex vert1 = new DBCMVertex(st.getSubject()+"");
					vertStore.put(st.getSubject(), vert1);
				}
				if(vertStore.get(st.getObject()) == null)
				{
					DBCMVertex vert2 = new DBCMVertex(st.getObject()+"");
					vertStore.put(st.getObject(),vert2);
				}
				//DBCMVertex vert
				//vert1 = getCoreData(vert1);
				//vert2 = getCoreData(vert2);
				
				//logger.info(vert1.getPropertyKeys());
				//logger.info(vert2.getPropertyKeys());
				//logger.info(vertStore);
				if(!vertStore.contains(st.getSubject() + "_" + st.getObject()) && !vertStore.contains(st.getObject() + "_" + st.getSubject()) && !vertStore.contains(st.getPredicate()) )
					try{
						df.addEdge(st.getPredicate(), vertStore.get(st.getSubject()), vertStore.get(st.getObject()));						
					}catch(Exception ignored){}
				//df.addEdge("3", "1","2");
				vertStore.put(st.getSubject() + "_" + st.getObject(),"true");
				vertStore.put(st.getObject() + "_" + st.getSubject(), "true");
				vertStore.put(st.getPredicate(), "true");
			}
			// get the core data for the vertices
			// Iterator it = df.getVertices().iterator();
			// will get properties on the fly when the user clicks
		}
		return df;
	}
	
	/**
	 * Method getCoreData.
	 * @param vert DBCMVertex
	
	 * @return DBCMVertex */
	public DBCMVertex getCoreData(DBCMVertex vert) throws Exception
	{
		//String subjectURI = vert.getURI() + "";
		//String subject = testToken(subjectURI+"");
		//String type = subjectURI.substring(0,subjectURI.indexOf(subject) - 1);
		String queryString = "CONSTRUCT {?x ?y ?z.} WHERE" + " { " +
	    //"{?x <" + RDF.TYPE + "> <" + type + "> ;} " + 
	    "{?x <" + RDFS.LABEL + "> \"AHLTA" +	/*subject +*/ "\" ;} " +
 		"{?y <"+RDF.TYPE + "> <http://semoss.org/ontologies/Relation/Contains> ;}" +
 		"{?x ?y ?z .}" +
 		"}";
		
		logger.info("Query is " + queryString);

		GraphQueryResult res = doRemoteSPARQL(queryString);
		while(res.hasNext())
		{
			Statement st = res.next();
			URI pred = st.getPredicate();
			logger.info(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			String propName = testToken(pred+"");
			logger.info(propName + "<<>>" + st.getObject());
			//vert.setProperty(propName, st.getObject());
		}
		return vert;
	}
	
	/**
	 * Method painGraph.
	 * @param f Forest
	 */
	public void painGraph(Forest f)
	{
		Layout daLayout = new FRLayout((edu.uci.ics.jung.graph.Graph)f);
		Renderer r = new BasicRenderer();
		VisualizationViewer vv = new VisualizationViewer(daLayout);
		
		vv.setRenderer(r);
		vv.getRenderContext().setVertexLabelTransformer(new SPVertexLabelTransformer());
		JFrame frame = new JFrame();
		frame.getContentPane().add(vv);
		frame.pack();
		frame.setVisible(true);
	}



	/**
	 * Method testToken.
	 * @param uri String
	
	 * @return String */
	public String testToken(String uri) {
		StringTokenizer tokens = new StringTokenizer(
				uri, "/");
		int totalTok = tokens.countTokens();
		String className = null;
		String instanceName = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok)
				className = tokens.nextToken();
			else if (tokIndex + 1 == totalTok)
				instanceName = tokens.nextToken();
			else
				tokens.nextToken();

		}
		//logger.info(" Class Name " + className + "<>" + instanceName);
		return instanceName;
	}


	/**
	 * Method readDataAsRDF.
	 */
	public void readDataAsRDF() throws Exception {
		CloseableIteration results;
		/*
		 * logger.debug("get statements: ?s ?p ?o ?g"); results = sc.getStatements(null, null, null, false);
		 * while(results.hasNext()) { logger.debug(results.next()); }
		 */

		/*
		 * logger.debug("\nget statements: http://pk.com#knows ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#knows2"), null, null, true); while(results.hasNext()) {
		 * logger.debug(results.next()); }
		 * 
		 * logger.debug("\nget statements: http://pk.com#1 ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#1"),null, null, false); while(results.hasNext()) {
		 * logger.debug(results.next()); }
		 */
		logger.debug("\nget statements: http://pk.com#connection ?p ?o ?g");
		
		results = sc.getStatements(null,
				vf.createURI("http://semoss.org/ontologies/Relation"),
				null, false);
		while (results.hasNext()) {
			logger.debug(results.next());
		}

		/*
		 * logger.debug("\nget statements: http://pk.com#entity ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#entity"),null, null, false); while(results.hasNext()) {
		 * logger.debug(results.next()); }
		 */

	}


	/**
	 * Method testTemp.
	 */
	public void testTemp()
	{
		//StringTemplateGroup stg = new StringTemplateGroup(
		ST st = new ST("Hello, $name2$", '$', '$');
		st.add("name2", "Yo Yo");
		logger.debug("Trying Templating");
		st.remove("name2");
		st.add("name2", "Haha");
		Map atMap = st.getAttributes();
		Iterator keys = atMap.keySet().iterator();
		while(keys.hasNext())
		{
			logger.debug(keys.next());
		}
		
		String tester = "Hello @pk@ haha @dumb@ this is cool";
		tester = "\"@pktemp     haha@\" ?system1 ?upstream ?icd. ?icd ?downstream ?system2.} WHERE { {?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?system1 <http://www.w3.org/2000/01/rdf-schema#label> \"@system1@\"; }{?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?system2 <http://www.w3.org/2000/01/rdf-schema#label> \"[System2]\";}{?system1 ?upstream ?icd ;}{?icd ?downstream ?system2 .}}";
		
		//Pattern pattern = Pattern.compile("[@]{1}\\w+\\s*@");
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[ ]*\\w+@");
		Matcher matcher = pattern.matcher(tester);
		String test2 = null;
		while(matcher.find())
		{
			String data = matcher.group();
			tester = tester.replace(data, "@Yo@");
			//data = data.substring(1,data.length()-1);
			logger.info(data);
			logger.info(matcher.start() + "<>" + matcher.end());
			logger.info("Count " + matcher.groupCount());
		}
		logger.info(st.render());
		logger.info("New tester is " + tester);
	}
	// load the RDF
	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String[] args) throws Exception {
		try {
			BigDataRDFTester tester = new BigDataRDFTester();
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String db = workingDir + "/BigData.Properties";

			//tester.testToken();
			
			tester.loadProperties(db);
			tester.openGraph();
			
			// tester.createRDFData();
			// tester.createGraphData();
			// tester.createSubClassTest();
			// tester.readDataAsRDF();
			// tester.doSPARQL();
			// tester.readRDFAsGraph();
			// tester.readDataAsGraph();

			
			String queryString = "CONSTRUCT {?x ?y ?z. ?p ?q ?z.} WHERE" + " { " +
		    "{?z <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/DataObject>;} " +
	 		"{?y <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Payload> ;}" +
	 		"{?x <" + RDFS.LABEL + "> \"PHARM-ART-Allergy Information\"; }" +
	 		"{?z <" + RDFS.LABEL + "> \"Allergy Information\";}" + 
	 		" {?x ?y ?z ;}" +
	 		"{?p <"+RDF.TYPE +"> <http://semoss.org/ontologies/Concept/Activity>;}" +
	 		"{?p <" + RDFS.LABEL + "> \"Treat Patient\";}"+
	 		"{?p ?q ?z .}" +
	 		"}";
			
			String systemNeighbors = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?carries ?data} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"ABTS\"; }" +
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd ?carries ?data .}"+
	 		"}";
			
			
			String a1 = "SELECT ?Subject ?Predicate ?Object WHERE {SELECT DISTINCT ?Subject WHERE {{?Predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://semoss.org/ontologies/Relation>;} {?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept>;} {?Subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept>;}}BINDINGS ?Subject{(<http://semoss.org/ontologies/Concept/Capability/Anesthesia_Documentation>)}} BINDINGS ?Predicate {(<http://semoss.org/ontologies/Relation/Fulfill>)}";


			String yo = "SELECT ?Subject ?Predicate ?Object WHERE { " +
					"SELECT DISTINCT ?Subject WHERE {" +
					"{?Predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://semoss.org/ontologies/Relation>;} {?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept>;} {?Subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept>;} {?Subject ?Predicate ?Object}}BINDINGS ?Subject{(<http://semoss.org/ontologies/Concept/Capability/Anesthesia_Documentation>)}" +
					"SELECT DISTINCT ?Predicate WHERE {{?Subject ?Predicate ?Object}} BINDINGS ?Predicate {(<http://semoss.org/ontologies/Relation/Fulfill>)}";
			
			
			
			String systemSytemInterface = "PREFIX dbcmConcept:	<http://semoss.org/ontologies/Concept/System> CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2.} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"dbcmConcept:System ;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"DMLSS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://semoss.org/ontologies/Relation/Payload>;} " +
	 		"{?data <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
//	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 .}" +
	 		"}";
			
			String systemSytemInterface2 = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2. ?icd ?carries ?data} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd ?carries ?data .}"+
	 		"}";

			
			
			String systemInformation = "CONSTRUCT {?system1 ?upstream ?icd. ?icd2 ?downstream2 ?system1. ?icd ?downstream ?system2. ?system3 ?upstream2 ?icd. ?icd ?carries ?data1. ?icd2 ?carries ?data2} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
		    "{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
		    "{?system3 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?upstream2 <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?icd2 <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"DMLSS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?downstream2 <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://semoss.org/ontologies/Relation/Payload>;} " +
	 		"{?data1 <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
	 		"{?data2 <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd2 ?downstream2 ?system1;}" +
	 		"{?system3 ?upstream2 ?icd2;}" +
	 		"{?icd ?carries ?data1;}" +
	 		"{?icd2 ?carries ?data2;}" +
	 		"}";

			String systemInformation2 = "CONSTRUCT {?system1 ?upstream ?icd. ?icd2 ?downstream ?system1. ?icd ?downstream ?system2. ?system3 ?upstream2 ?icd. ?icd ?carries ?data1. ?icd2 ?carries ?data2} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
		    "{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
		    "{?system3 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?upstream2 <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?icd2 <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?downstream2 <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://semoss.org/ontologies/Relation/Payload>;} " +
	 		"{?data1 <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
	 		"{?data2 <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd2 ?downstream2 ?system1;}" +
	 		"{?system3 ?upstream2 ?icd2;}" +
	 		"{?icd ?carries ?data1;}" +
	 		"{?icd2 ?carries ?data2;}" +
	 		"}";
			
			String systemProp = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2. ?icd ?carries ?data1. ?system1 ?contains 								?attribute1. ?system2 ?contains2 ?attribute2. ?icd ?contains3 ?attribute3} \n WHERE { {?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} \n {?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} \n {?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} \n {?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} \n {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} \n {?system1 <http://www.w3.org/2000/01/rdf-schema#label> \"ABTS\"; } {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;}{?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;}{?contains3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;}{?system1 ?upstream ?icd ;}{?icd ?downstream ?system2 ;} \n {?icd ?carries ?data1;} \n  \n  {?system1 ?contains ?attribute1;} \n {?system2 ?contains2 ?attribute2;} OPTIONAL {?icd ?contains3 ?attribute3.}   }";
			
			String icdProp = "CONSTRUCT {?icd ?contains3 ?attribute3} \n WHERE {{?icd <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http://semoss.org/ontologies/Relation> ;} {?contains3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://semoss.org/ontologies/Relation/Contains>;} {?icd ?contains3 ?attribute3.}   }";
			
			
			//?system1 ?contains 								?attribute1. ?system2 ?contains2 ?attribute2.{?system1 ?contains ?attribute1;} \n {?system2 ?contains2 ?attribute2;}
			
			String url2Encode = URLEncoder.encode("ABTS-CHCS-Patient ID", "UTF-8");
			String typeQuery = "SELECT ?entity WHERE {?entity <" + RDF.TYPE + ">  <http://semoss.org/ontologies/Concept/System>;}";
			
			String icdDataQuery = "CONSTRUCT {?interface ?carries ?data. ?interface ?label ?name} WHERE { {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?interface ?carries ?data;} {?interface ?label ?name;}" + "FILTER regex(?name, \"CHCS\") }";
					//"Filter (?name in (\"ABTS-CHCS-Order Information\",\"ABTS-CHCS-Patient Demographics and Information\",\"ABTS-CHCS-Patient ID\",\"ABTS-CHCS-Patient Test Results\"))}";
			
			
			
			
			String icdDataQuery2 = "CONSTRUCT {?interface ?carries ?data.} WHERE { " +
					"{?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} " +
					"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?interface ?carries ?data;} Filter (?interface in (<http://semoss.org/ontologies/Concept/InterfaceControlDocument/ABTS-CHCS-Order Information>,<http://semoss.org/ontologies/Concept/InterfaceControlDocument/ABTS-CHCS-Patient Demographics and Information>,<http://semoss.org/ontologies/Concept/InterfaceControlDocument/ABTS-CHCS-Patient ID>,<http://semoss.org/ontologies/Concept/InterfaceControlDocument/ABTS-CHCS-Patient Test Results>))}";
			
			String systemBLData = "CONSTRUCT {?system ?provide ?data.  ?system ?provide2 ?BL} WHERE {" +
				"{?system <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
				"{?provide <" + RDFS.SUBPROPERTYOF + "> " +	"<http://semoss.org/ontologies/Relation/Provide>;} " +  
				"{?provide2 <" + RDFS.SUBPROPERTYOF + "> " +	"<http://semoss.org/ontologies/Relation/Provide>;} " +
		 		"{?data <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/DataObject>;}"+
		 		"{?BL <" + RDF.TYPE + "> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}"+
		 		"{?system <" + RDFS.LABEL + "> \"BHIE\"; }" +
		 		"{?system ?provide ?data.} {?system2 ?provide2 ?BL.}}";
				
			String swQuery = "CONSTRUCT {?system ?has ?softwareModule.} WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} " +
					"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} " +
					"{?softwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;} " +
					"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} " +
					"{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/SoftwareModuleForSoftwareVersion>;} " +
					"{?system ?has ?softwareModule;} {?softwareModule ?typeof ?softwareVersion;} {?softwareVersion ?label ?name;}" + "FILTER regex(?name, \"Oracle\", \"i\") }";
				
			String swQuerySelect = "SELECT DISTINCT ?system ?softwareModule WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} " +
			"{?softwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareVersion> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} " +
			"{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/SoftwareModuleForSoftwareVersion>;} " +
			"{?system ?has ?softwareModule;} {?softwareModule ?typeof ?softwareVersion;} {?softwareVersion ?label ?name;}" + "FILTER regex(?name, \"Oracle\", \"i\") }";
			
			String swSystemQuerySelect = "SELECT DISTINCT ?system ?softwareModule WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} " +
			"{?system ?has ?softwareModule;} {?system <" +  RDFS.LABEL + "> ?name } FILTER regex(?name, \"DMLSS\", \"i\") }";
			//icdDataQuery = URLEncoder.encode(icdDataQuery, "UTF-8");//(icdDataQuery);
			String swSystemQuerySelect2 = "SELECT DISTINCT ?system ?softwareModule WHERE { " +
					"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareModule> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} " +
			"{?system <" +  RDFS.LABEL + "> \"DMLSS\" }" +
			"{?system ?has ?softwareModule;} }";
			
			String neighborICDHoodQuery = "CONSTRUCT {?focus ?predicate ?object. ?subject ?predicate2 ?focus} WHERE {" +
					"{?focus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" +
					"{?focus2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" + 
					"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}" + 
					"{?predicate2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume> ;}" + 
					"{?focus <" +  RDFS.LABEL + "> ?name ;}" + 
					"{?focus ?predicate ?object}" + 
					"{?subject ?predicate2 ?focus}" +
					"FILTER (?name in (\"DMLSS\"))" +
					"}";
			
			String neighborICDHoodQuery2 = "SELECT  DISTINCT ?focus ?predicate ?object WHERE {" +
			"{?focus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" +
			"{?focus2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" +
			"{?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}" +
			"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}" + 
			"{?predicate2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume> ;}" + 
			"{?focus <" +  RDFS.LABEL + "> ?name ;}" + 
			"{?focus ?predicate ?object}" + 
			//"{?subject ?predicate2 ?focus}" +
			"FILTER (?name in (\"ABTS\"))" +
			"}";

			String capReq = "CONSTRUCT {?cap ?help ?req. ?cap ?label ?name} WHERE " +
					"{ {?req <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Requirement>;} " +
					"{?help <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/FulFill>;} " +
					"{?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} " +
					"{?cap ?help ?req;} {?bp <http://www.w3.org/2000/01/rdf-schema#label> ?name;} " +
					"Filter (?name in (\"Laboratory\"))}";
			
			String serviceData = "CONSTRUCT {?service ?provide ?data} WHERE " +
					"{ {?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>}" +
					" {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>} " +
					" {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}" +
					"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" +
					"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Upstream> ;}" +
					"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
					"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}" +
			 		//"{?system <" + RDFS.LABEL + "> \"DMLSS\"; }" +
			 		"{?system ?upstream ?icd;}" + 
			 		"{?icd ?carries ?data;}" + 
			 		"{?service ?provide ?data}"+
			 		"}" ;
					

			String serviceData2 = "CONSTRUCT " + "{?system ?upstream ?icd}"+
					//"{?service ?provide ?data}" +
					//"{?system ?upstream ?icd. ?icd ?carries ?data} " +
					"WHERE " +
			"{ " +
			"{?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>}" +
			" {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>} " +
			" {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}" +
			"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}" +
			"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide> ;}" +
			"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
			"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>}" +
	 		"{?service <" + RDFS.LABEL + "> \"Equipment Monitoring Service\"; }" +
	 		"{?system ?upstream ?icd;}" + 
	 		"{?icd ?carries ?data;}" + 
	 		"{?service ?provide ?data}"+
	 		"}" ;

			
			/*+ 
					"FILTER regex(?name, \"ADM eForms\", \"i\")" + 
					"FILTER regex(?name2, \"ADM eForms\", \"i\") }";
			*/
			//AHLTA-BHIE-Patient Demographics and Information
			/*String labMigrate = "CONSTRUCT {?businessprocess ?consist ?activity. " +
											"?activity ?need ?data. " +
											"?data ?provide ?system1. " +
											"?system1 ?upstream ?icd. " +
											"?icd ?downstream ?system2.} " +
											"?service ?provide ?data.} " +
											"WHERE" + " { " +
											"{?businessprocess <" + RDFS.LABEL + "> " +	"<http://semoss.org/ontologies/Concept/BusinessProcess/>;} " +
									 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
									 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
			"{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 .}" +
	 		"}";
			*/
			
			String bindQ = "SELECT  DISTINCT ?predicate WHERE " +
					"{?subject ?predicate ?object. " +
					"BIND (<http://semoss.org/ontologies/Concept/System/ABTS>" +
					" AS ?subject).}";

			String bindQ4 = "SELECT  ?subject ?predicate ?object WHERE " +
			"{{?subject ?predicate ?object.} " +
			"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}"+
			"BIND (<http://semoss.org/ontologies/Concept/System/ABTS>" +
			" AS ?subject).}";

			String bindQ2 = "SELECT ?upstream   WHERE { BIND (<http://semoss.org/ontologies/Concept/System/ABTS>  AS ?sys ). " +
					"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} " +
					"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http://semoss.org/ontologies/Relation/Provide>;} {?sys ?upstream ?icd .} }";
			
			String bindQ3 = "SELECT  ?predicate WHERE { " +
					"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http:/health.mil/ontologies/dbcm/Relation> ;} " +
					//"BIND (<http://semoss.org/ontologies/Concept/System/CHCS> AS ?subject). " +
					"}";
			
			String bindQ5 = "SELECT  ?subject ?predicate ?object WHERE {{?subject ?predicate ?object.} {?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>.} BIND (<http://semoss.org/ontologies/Concept/System/ABTS> AS ?subject).}";
			String bindQ6 = "CONSTRUCT  {?subject ?predicate ?object} WHERE {{?object  <http://www.w3.org/2000/01/rdf-schema#label> \"CRUD Procedure Order\";} {?subject <http://www.w3.org/2000/01/rdf-schema#label> \"ABTS-CHCS-Order Information\";} {?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;} {?subject ?predicate ?object.} }";
			
			String bindQ7 = "SELECT  ?subject ?predicate ?object WHERE {BIND (<http://semoss.org/ontologies/Concept/Service/AlertOnAbnormalValue> AS ?subject). ?subject ?predicate ?object;}";
			
			String tommyT = "INSERT {<http://semoss.org/ontologies/Concept/Service/AlertOnAbnormalValue><http://semoss.org/ontologies/Relation/Exposes><http://semoss.org/ontologies/Concept/DataObject/tom1>} WHERE{}";
			String tommyT2 = "INSERT { <http://semoss.org/ontologies/Concept/DataObject/tom1><http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}WHERE{}"; 

			//logger.debug(" Query >>> \n" + serviceData2);
			//Forest f = tester.convertSailtoGraph(res);
			//tester.painGraph(f);
			tester.getCoreData(null);
			String [] names = {"focus"}; //,"predicate", "object"};
			//tester.doRemoveSelect(bindQ7,names);
			tester.execInsertQuery(tommyT2);
			tester.closeGraph();
			tester.testTemp();

			
			//SELECT  DISTINCT ?predicate WHERE {BIND (<http://semoss.org/ontologies/Concept/InterfaceControlDocument/ABTS-CHCS-Patient ID> AS ?subject). {?subject ?predicate ?object.}}
		} catch (Exception ignored) {
			System.err.println("Exception " + ignored);
			ignored.printStackTrace();
		}

	}
	/**
	 * Method execInsertQuery.
	 * @param query String
	 */
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException {

		try {
			Update up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
			logger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			rc.setAutoCommit(false);
			up.execute();
			//rc.commit();
	        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
	        ie.computeClosure(null);
			sc.commit();
			
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		}

	}
}
