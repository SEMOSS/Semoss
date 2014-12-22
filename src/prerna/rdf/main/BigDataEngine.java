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
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryConnection;
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
public class BigDataEngine {
	
	static final Logger logger = LogManager.getLogger(BigDataEngine.class.getName());

	BigdataSail bdSail = null;
	Properties bdProp = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
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

		// new ForwardChainingRDFSInferencer(bdSail);
		InferenceEngine ie = bdSail.getInferenceEngine();
		logger.info("ie forward chaining " + ie);
		//logger.info("Truth " + bdSail.isTruthMaintenance());
		logger.info("quad " + bdSail.isQuads());
		// logger.info("ie forward chaining " + ie);

		// SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();

		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		
		sc = ((SailRepositoryConnection) rc).getSailConnection();
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
	 * while (i.hasNext()) { logger.debug("statement " + i.next()); } } finally { i.close(); } } finally {
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
		 * logger.info("The URI is " + uriIt.next()); }
		 */

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
		// logger.info("result is " + res.);
		/*while (res.hasNext()) {
			//BindingSet bs = res.next();
			Statement st = res.next();
			logger.info(st.getSubject() + "<>" + st.getPredicate()
					+ "<>" + st.getObject());
			//logger.debug("Binding is " + bs);
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
			//logger.debug("Iterating ...");
			Statement st = res.next();
			
			String predicate = st.getPredicate()+"";
			//logger.info(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
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
				if(!vertStore.contains(st.getSubject() + "_" + st.getObject()) && !vertStore.contains(st.getPredicate()) )
					df.addEdge(st.getPredicate(), vertStore.get(st.getSubject()), vertStore.get(st.getObject()));
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
		String subjectURI = vert.getURI() + "";
		String subject = testToken(subjectURI+"");
		String type = subjectURI.substring(0,subjectURI.indexOf(subject) - 1);
		String queryString = "CONSTRUCT {?x ?y ?z.} WHERE" + " { " +
	    "{?x <" + RDF.TYPE + "> <" + type + "> ;} " + 
	    "{?x <" + RDFS.LABEL + "> \"" +	subject + "\" ;} " +
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
			vert.setProperty(propName, st.getObject());
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

/*	public void createGraphData() throws Exception {
		org.neo4j.graphdb.Transaction tx = svc.beginTx();
		try {
			Node newNode = svc.createNode();
			newNode.setProperty("kind", "uri");
			newNode.setProperty("value", "http://pk.com#4");
			tx.success();
			tx.finish();
		} catch (Exception ex) {
			System.err.println("Error ");
			ex.printStackTrace();
		}
	}*/

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

/*	public void readDataAsGraph() throws Exception {
		// org.neo4j.graphdb.Transaction tx = svc.beginTx();

		Iterator vs = ng.getVertices().iterator();
		while (vs.hasNext()) {
			Vertex v = (Vertex) vs.next();
			Iterator ki = v.getPropertyKeys().iterator();
			logger.debug("Vertex >>>>");
			while (ki.hasNext()) {
				String thiski = (String) ki.next();
				Object val = v.getProperty(thiski);
				logger.debug(thiski + "<<>>" + val);
			}
			v.setProperty("name", "Hello World");

			/*
			 * Iterator edges = v.getEdges("").iterator(); while(edges.hasNext()) { Edge e = (Edge)edges.next();
			 * Iterator ei = e.getPropertyKeys().iterator(); logger.debug("Edge >>>>"); while(ei.hasNext()) {
			 * String thiski = (String)ei.next(); Object val = v.getProperty(thiski); logger.debug(thiski + "<<>>"
			 * + val); } }
		}

		Iterator rs = ng.getEdges().iterator();

		while (rs.hasNext()) {
			Edge e = (Edge) rs.next();
			Iterator ki = e.getPropertyKeys().iterator();
			logger.debug("Edge >>>>");
			Vertex vi = e.getInVertex();
			Vertex vo = e.getOutVertex();
			logger.debug("Vertices >> " + vi.getProperty("value")
					+ "<<>>" + vo.getProperty("value"));
			while (ki.hasNext()) {
				String thiski = (String) ki.next();
				Object val = e.getProperty(thiski);
				logger.debug(thiski + "<<>>" + val);
			}
		}

		// tx.success();
		// tx.finish();
	}
*/
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
		
		Pattern pattern = Pattern.compile("@[A-Za-z]*@");
		Matcher matcher = pattern.matcher(tester);
		String test2 = null;
		while(matcher.find())
		{
			String data = matcher.group();
			logger.debug(data);
			tester = tester.replace(data, "Yo");
			logger.debug(matcher.start() + "<>" + matcher.end());
			logger.debug("Count " + matcher.groupCount());
		}
		logger.debug(st.render());
		logger.debug("New tester is " + tester);
	}
	// load the RDF
	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String[] args) throws Exception {
		try {
			BigDataEngine tester = new BigDataEngine();
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
			

			String systemSytemInterface = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2.} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://semoss.org/ontologies/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://semoss.org/ontologies/Concept/System>;} " +
	 		/*"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+*/
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 .}" +
	 		"}";

			
			GraphQueryResult res = tester.doRemoteSPARQL(systemSytemInterface);
			Forest f = tester.convertSailtoGraph(res);
			tester.painGraph(f);
			
			tester.closeGraph();
			tester.testTemp();

		} catch (RuntimeException ignored) {
			System.err.println("Exception " + ignored);
			ignored.printStackTrace();
		}

	}

}
