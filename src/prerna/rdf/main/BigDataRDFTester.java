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

public class BigDataRDFTester {

	BigdataSail bdSail = null;
	Properties bdProp = null;
	Properties rdfMap = null;
	SailRepositoryConnection rc = null;
	SailConnection sc = null;
	ValueFactory vf = null;

	public void openGraph() throws Exception {

		bdSail = new BigdataSail(bdProp);
		// BigdataSail.Options.TRUTH_MAINTENANCE = "true";
		BigdataSailRepository repo = new BigdataSailRepository(bdSail);
		repo.initialize();
		rc = repo.getConnection();

		//bdSail.
		// new ForwardChainingRDFSInferencer(bdSail);
		InferenceEngine ie = bdSail.getInferenceEngine();
		System.out.println("ie forward chaining " + ie);
		System.out.println("Truth " + bdSail.isTruthMaintenance());
		System.out.println("quad " + bdSail.isQuads());
		
		// System.out.println("ie forward chaining " + ie);

		// SailRepositoryConnection src = (SailRepositoryConnection) repo.getConnection();

		String workingDir = System.getProperty("user.dir");
		
		sc = (rc).getSailConnection();
		String propFile = workingDir + "/DBCM_RDF_Map.prop";
		loadRDFMap(propFile);

		// sail = new ForwardChainingRDFSInferencer(new GraphSail(ng));
		// sail.initialize();
		// sc = sail.getConnection();
		vf = bdSail.getValueFactory();
		// return g;
	}

	public void loadProperties(String fileName) throws Exception {
		bdProp = new Properties();
		bdProp.load(new FileInputStream(fileName));
		System.out.println("Properties >>>>>>>>" + bdProp);

	}
	
	public void loadRDFMap(String fileName) throws Exception
	{
		rdfMap = new Properties();
		rdfMap.load(new FileInputStream(fileName));
	}

	public void closeGraph() throws Exception {
		// ng.stopTransaction(Conclusion.SUCCESS);
		bdSail.shutDown();
		// ng.shutdown();
	}

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
						System.out.println("statement " + i.next());
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
	 * while (i.hasNext()) { System.out.println("statement " + i.next()); } } finally { i.close(); } } finally {
	 * //sc.close(); } } finally { //reasoner.shutDown(); } }
	 */

	public void doSPARQL() throws Exception {

		SPARQLParser parser = new SPARQLParser();
		// CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;
		// String queryString =
		// "SELECT ?x ?y WHERE {?x <http://health.mil/ontologies/dbcm/Relation/Supports/Immunization_Immunizations> ?y}";
		// // +
		// " <http://pk.com#1> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?y." +
		// " }";
		// String queryString =
		// "SELECT ?x WHERE {<http://health.mil/ontologies/dbcm/Concept/Capability/Immunization>  <"+RDF.TYPE+"> ?x}";
		// // +
		// String queryString =
		// "SELECT ?x WHERE {<http://health.mil/ontologies/dbcm/Relation/Supports/Immunization_Immunizations> <http://health.mil/ontologies/dbcm/Relation> ?x}";
		String queryString = "SELECT * WHERE {?y <http://health.mil/ontologies/dbcm/Relation/Contains> ?x}";
		// SparqlBuilder
		// String queryString2 = "SELECT * FROM {Cap}  " + "<http://health.mil/ontologies/dbcm/Relation/Supports>" +
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

		System.out.println("\nSPARQL: " + queryString);
		tq.setIncludeInferred(true /* includeInferred */);
		// gq.evaluate();
		// sparqlResults = sc.evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
		TupleQueryResult sparqlResults = tq.evaluate();
		while (sparqlResults.hasNext()) {
			System.out.println("Inside");
			BindingSet bs = sparqlResults.next();
			System.out.println(bs);
		}
		/*
		 * Iterator uriIt = tq.getDataset().getDefaultGraphs().iterator(); while(uriIt.hasNext()) {
		 * System.out.println("The URI is " + uriIt.next()); }
		 */

	}
	
	public void doRemoveSelect(String queryString, String [] names) throws Exception
	{
		System.out.println("\nSPARQL: " + queryString);
		TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		tq.setIncludeInferred(true /* includeInferred */);
		// gq.evaluate();
		// sparqlResults = sc.evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
		SPARQLResultsJSONWriter w = new SPARQLResultsJSONWriter(System.out);
		TupleQueryResult sparqlResults = 
		tq.evaluate();
		//sparqlResults.
		
		names = null;
		
		System.out.println("Size is " + tq.getBindings().size());
		
		
		while (sparqlResults.hasNext()) {
			System.out.println(">>>>> ");
			BindingSet bs = sparqlResults.next();
			System.out.println(" Binding set binding names " + bs.getBindingNames().size());
			//String name2 = bs.getBindingNames().iterator().next();
			//System.out.println(" name value is " + name2);
			//System.out.println(" Value is " + bs.getValue(name2) + "");
			if(names == null)
			{
				names = new String[bs.getBindingNames().size()];
				Iterator nameIt = bs.getBindingNames().iterator();
				for(int nameIndex = 0;nameIndex < names.length; nameIndex++)
				{
					String name = nameIt.next() + "";
					System.out.println("Name is  " + name);
					names[nameIndex] = name;
					
				}

			}
			for(int nameIndex = 0;nameIndex < names.length;nameIndex++)
				if(nameIndex < names.length - 1)
					System.out.print(bs.getValue(names[nameIndex]) + "\t");
				else
					System.out.print(bs.getValue(names[nameIndex]));
		}
		
	}

	public GraphQueryResult doRemoteSPARQL(String queryString) throws Exception {
		// String queryString = "SELECT * WHERE {?y <http://health.mil/ontologies/dbcm/Relation/Contains> ?x}";
		// String queryString = "CONSTRUCT {?fromObjectUri ?relationUri ?o .}WHERE" +
		// " { {?fromObjectUri ?relationUri ?o .} }";
		// String queryString = "CONSTRUCT {?x <" + RDFS.SUBCLASSOF +
		// "> <http://health.mil/ontologies/dbcm/Concept/Lifecycle> .} WHERE" + " { {?x <" + RDFS.SUBCLASSOF +
		// "> ?y .} }";

		 //String queryString = "SELECT ?y WHERE" + " {?y <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/BusinessProcess>}";
		 
		 
		 /* Final Working Copy
		  * String queryString = "CONSTRUCT {?x ?y ?z.} WHERE" + " { {?z <" + RDF.TYPE + "> " +
		 		"<http://health.mil/ontologies/dbcm/Concept/DataObject>;} " +
		 		"{?y <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Payload> ;}" + 
		 		" {?x ?y ?z .}}";
		 */
		
		//String queryString = "CONSTRUCT {?x ?y ?z .} WHERE "
		//		+ "{ {?x <"+ RDFS.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/BusinessProcess> .}}";


		 //TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		
		 GraphQuery sagq = rc.prepareGraphQuery(QueryLanguage.SPARQL,
				queryString);
		//sagq.setBinding("o", vf.createURI("http://health.mil/ontologies/dbcm/Concept/Lifecycle"));
		//sagq.setBinding("relationUri", RDFS.SUBCLASSOF);

		GraphQueryResult res = sagq.evaluate();
		// TupleQueryResult res = tq.evaluate();

		return res;
		// System.out.println("result is " + res.);
		/*while (res.hasNext()) {
			//BindingSet bs = res.next();
			Statement st = res.next();
			System.out.println(st.getSubject() + "<>" + st.getPredicate()
					+ "<>" + st.getObject());
			//System.out.println("Binding is " + bs);
		}*/
	}
	
	public Forest convertSailtoGraph(GraphQueryResult res) throws Exception
	{
		DelegateForest df = new DelegateForest(); 
		Hashtable vertStore = new Hashtable();
		while(res.hasNext())
		{
			//System.out.println("Iterating ...");
			Statement st = res.next();
			
			String predicate = st.getPredicate()+"";
			System.out.println(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			if(predicate.contains("Relation") && !rdfMap.contains(predicate) ) // need to change this to starts with relation
			{
				//System.out.println("Adding" + st.getPredicate());
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
				
				//System.out.println(vert1.getPropertyKeys());
				//System.out.println(vert2.getPropertyKeys());
				//System.out.println(vertStore);
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
	
	public DBCMVertex getCoreData(DBCMVertex vert) throws Exception
	{
		//String subjectURI = vert.getURI() + "";
		//String subject = testToken(subjectURI+"");
		//String type = subjectURI.substring(0,subjectURI.indexOf(subject) - 1);
		String queryString = "CONSTRUCT {?x ?y ?z.} WHERE" + " { " +
	    //"{?x <" + RDF.TYPE + "> <" + type + "> ;} " + 
	    "{?x <" + RDFS.LABEL + "> \"AHLTA" +	/*subject +*/ "\" ;} " +
 		"{?y <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Relation/Contains> ;}" +
 		"{?x ?y ?z .}" +
 		"}";
		
		System.out.println("Query is " + queryString);

		GraphQueryResult res = doRemoteSPARQL(queryString);
		while(res.hasNext())
		{
			Statement st = res.next();
			URI pred = st.getPredicate();
			System.out.println(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			String propName = testToken(pred+"");
			System.out.println(propName + "<<>>" + st.getObject());
			//vert.setProperty(propName, st.getObject());
		}
		return vert;
	}
	
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
		//System.out.println(" Class Name " + className + "<>" + instanceName);
		return instanceName;
	}


	public void readDataAsRDF() throws Exception {
		CloseableIteration results;
		/*
		 * System.out.println("get statements: ?s ?p ?o ?g"); results = sc.getStatements(null, null, null, false);
		 * while(results.hasNext()) { System.out.println(results.next()); }
		 */

		/*
		 * System.out.println("\nget statements: http://pk.com#knows ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#knows2"), null, null, true); while(results.hasNext()) {
		 * System.out.println(results.next()); }
		 * 
		 * System.out.println("\nget statements: http://pk.com#1 ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#1"),null, null, false); while(results.hasNext()) {
		 * System.out.println(results.next()); }
		 */
		System.out
				.println("\nget statements: http://pk.com#connection ?p ?o ?g");
		// sc.
		results = sc.getStatements(null,
				vf.createURI("http://health.mil/ontologies/dbcm/Relation"),
				null, false);
		while (results.hasNext()) {
			System.out.println(results.next());
		}

		/*
		 * System.out.println("\nget statements: http://pk.com#entity ?p ?o ?g"); results =
		 * sc.getStatements(vf.createURI("http://pk.com#entity"),null, null, false); while(results.hasNext()) {
		 * System.out.println(results.next()); }
		 */

	}


	public void testTemp()
	{
		//StringTemplateGroup stg = new StringTemplateGroup(
		ST st = new ST("Hello, $name2$", '$', '$');
		st.add("name2", "Yo Yo");
		System.out.println("Trying Templating");
		st.remove("name2");
		st.add("name2", "Haha");
		Map atMap = st.getAttributes();
		Iterator keys = atMap.keySet().iterator();
		while(keys.hasNext())
		{
			System.out.println(keys.next());
		}
		
		String tester = "Hello @pk@ haha @dumb@ this is cool";
		tester = "\"@pktemp     haha@\" ?system1 ?upstream ?icd. ?icd ?downstream ?system2.} WHERE { {?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>;} {?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}{?system1 <http://www.w3.org/2000/01/rdf-schema#label> \"@system1@\"; }{?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consume>;}{?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>;} {?system2 <http://www.w3.org/2000/01/rdf-schema#label> \"[System2]\";}{?system1 ?upstream ?icd ;}{?icd ?downstream ?system2 .}}";
		
		//Pattern pattern = Pattern.compile("[@]{1}\\w+\\s*@");
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[ ]*\\w+@");
		Matcher matcher = pattern.matcher(tester);
		String test2 = null;
		while(matcher.find())
		{
			String data = matcher.group();
			tester = tester.replace(data, "@Yo@");
			//data = data.substring(1,data.length()-1);
			System.out.println(data);
			System.out.println(matcher.start() + "<>" + matcher.end());
			System.out.println("Count " + matcher.groupCount());
		}
		System.out.println(st.render());
		System.out.println("New tester is " + tester);
	}
	// load the RDF
	public static void main(String[] args) throws Exception {
		try {
			BigDataRDFTester tester = new BigDataRDFTester();
			String workingDir = System.getProperty("user.dir");
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
		    "{?z <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/DataObject>;} " +
	 		"{?y <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Payload> ;}" +
	 		"{?x <" + RDFS.LABEL + "> \"PHARM-ART-Allergy Information\"; }" +
	 		"{?z <" + RDFS.LABEL + "> \"Allergy Information\";}" + 
	 		" {?x ?y ?z ;}" +
	 		"{?p <"+RDF.TYPE +"> <http://health.mil/ontologies/dbcm/Concept/Activity>;}" +
	 		"{?p <" + RDFS.LABEL + "> \"Treat Patient\";}"+
	 		"{?p ?q ?z .}" +
	 		"}";
			
			String systemNeighbors = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?carries ?data} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"ABTS\"; }" +
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd ?carries ?data .}"+
	 		"}";
			
			
			String a1 = "SELECT ?Subject ?Predicate ?Object WHERE {SELECT DISTINCT ?Subject WHERE {{?Predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://health.mil/ontologies/dbcm/Relation>;} {?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://health.mil/ontologies/dbcm/Concept>;} {?Subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://health.mil/ontologies/dbcm/Concept>;}}BINDINGS ?Subject{(<http://health.mil/ontologies/dbcm/Concept/Capability/Anesthesia_Documentation>)}} BINDINGS ?Predicate {(<http://health.mil/ontologies/dbcm/Relation/Fulfill>)}";


			String yo = "SELECT ?Subject ?Predicate ?Object WHERE { " +
					"SELECT DISTINCT ?Subject WHERE {" +
					"{?Predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://health.mil/ontologies/dbcm/Relation>;} {?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://health.mil/ontologies/dbcm/Concept>;} {?Subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://health.mil/ontologies/dbcm/Concept>;} {?Subject ?Predicate ?Object}}BINDINGS ?Subject{(<http://health.mil/ontologies/dbcm/Concept/Capability/Anesthesia_Documentation>)}" +
					"SELECT DISTINCT ?Predicate WHERE {{?Subject ?Predicate ?Object}} BINDINGS ?Predicate {(<http://health.mil/ontologies/dbcm/Relation/Fulfill>)}";
			
			
			
			String systemSytemInterface = "PREFIX dbcmConcept:	<http://health.mil/ontologies/dbcm/Concept/System> CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2.} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"dbcmConcept:System ;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"DMLSS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://health.mil/ontologies/dbcm/Relation/Payload>;} " +
	 		"{?data <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
//	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 .}" +
	 		"}";
			
			String systemSytemInterface2 = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2. ?icd ?carries ?data} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd ?carries ?data .}"+
	 		"}";

			
			
			String systemInformation = "CONSTRUCT {?system1 ?upstream ?icd. ?icd2 ?downstream2 ?system1. ?icd ?downstream ?system2. ?system3 ?upstream2 ?icd. ?icd ?carries ?data1. ?icd2 ?carries ?data2} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
		    "{?system2 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
		    "{?system3 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?upstream2 <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?icd2 <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"DMLSS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?downstream2 <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://health.mil/ontologies/dbcm/Relation/Payload>;} " +
	 		"{?data1 <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
	 		"{?data2 <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd2 ?downstream2 ?system1;}" +
	 		"{?system3 ?upstream2 ?icd2;}" +
	 		"{?icd ?carries ?data1;}" +
	 		"{?icd2 ?carries ?data2;}" +
	 		"}";

			String systemInformation2 = "CONSTRUCT {?system1 ?upstream ?icd. ?icd2 ?downstream ?system1. ?icd ?downstream ?system2. ?system3 ?upstream2 ?icd. ?icd ?carries ?data1. ?icd2 ?carries ?data2} WHERE" + " { " +
		    "{?system1 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
		    "{?system2 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
		    "{?system3 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?upstream2 <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?icd2 <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?downstream2 <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?carries <" + RDFS.SUBPROPERTYOF + "> " +	"<http://health.mil/ontologies/dbcm/Relation/Payload>;} " +
	 		"{?data1 <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
	 		"{?data2 <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 ;}" +
	 		"{?icd2 ?downstream2 ?system1;}" +
	 		"{?system3 ?upstream2 ?icd2;}" +
	 		"{?icd ?carries ?data1;}" +
	 		"{?icd2 ?carries ?data2;}" +
	 		"}";
			
			String systemProp = "CONSTRUCT {?system1 ?upstream ?icd. ?icd ?downstream ?system2. ?icd ?carries ?data1. ?system1 ?contains 								?attribute1. ?system2 ?contains2 ?attribute2. ?icd ?contains3 ?attribute3} \n WHERE { {?system1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>;} \n {?system2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>;} \n {?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide>;} \n {?downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consume>;} \n {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;} \n {?system1 <http://www.w3.org/2000/01/rdf-schema#label> \"ABTS\"; } {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>;}{?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>;}{?contains3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>;}{?system1 ?upstream ?icd ;}{?icd ?downstream ?system2 ;} \n {?icd ?carries ?data1;} \n  \n  {?system1 ?contains ?attribute1;} \n {?system2 ?contains2 ?attribute2;} OPTIONAL {?icd ?contains3 ?attribute3.}   }";
			
			String icdProp = "CONSTRUCT {?icd ?contains3 ?attribute3} \n WHERE {{?icd <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http://health.mil/ontologies/dbcm/Relation> ;} {?contains3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://health.mil/ontologies/dbcm/Relation/Contains>;} {?icd ?contains3 ?attribute3.}   }";
			
			
			//?system1 ?contains 								?attribute1. ?system2 ?contains2 ?attribute2.{?system1 ?contains ?attribute1;} \n {?system2 ?contains2 ?attribute2;}
			
			String url2Encode = URLEncoder.encode("ABTS-CHCS-Patient ID", "UTF-8");
			String typeQuery = "SELECT ?entity WHERE {?entity <" + RDF.TYPE + ">  <http://health.mil/ontologies/dbcm/Concept/System>;}";
			
			String icdDataQuery = "CONSTRUCT {?interface ?carries ?data. ?interface ?label ?name} WHERE { {?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Payload>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}{?interface ?carries ?data;} {?interface ?label ?name;}" + "FILTER regex(?name, \"CHCS\") }";
					//"Filter (?name in (\"ABTS-CHCS-Order Information\",\"ABTS-CHCS-Patient Demographics and Information\",\"ABTS-CHCS-Patient ID\",\"ABTS-CHCS-Patient Test Results\"))}";
			
			
			
			
			String icdDataQuery2 = "CONSTRUCT {?interface ?carries ?data.} WHERE { " +
					"{?interface <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;} " +
					"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Payload>;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}{?interface ?carries ?data;} Filter (?interface in (<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument/ABTS-CHCS-Order Information>,<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument/ABTS-CHCS-Patient Demographics and Information>,<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument/ABTS-CHCS-Patient ID>,<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument/ABTS-CHCS-Patient Test Results>))}";
			
			String systemBLData = "CONSTRUCT {?system ?provide ?data.  ?system ?provide2 ?BL} WHERE {" +
				"{?system <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
				"{?provide <" + RDFS.SUBPROPERTYOF + "> " +	"<http://health.mil/ontologies/dbcm/Relation/Provide>;} " +  
				"{?provide2 <" + RDFS.SUBPROPERTYOF + "> " +	"<http://health.mil/ontologies/dbcm/Relation/Provide>;} " +
		 		"{?data <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/DataObject>;}"+
		 		"{?BL <" + RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/BusinessLogicUnit>;}"+
		 		"{?system <" + RDFS.LABEL + "> \"BHIE\"; }" +
		 		"{?system ?provide ?data.} {?system2 ?provide2 ?BL.}}";
				
			String swQuery = "CONSTRUCT {?system ?has ?softwareModule.} WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;} " +
					"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareModule> ;} " +
					"{?softwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareVersion> ;} " +
					"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consists>;} " +
					"{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/SoftwareModuleForSoftwareVersion>;} " +
					"{?system ?has ?softwareModule;} {?softwareModule ?typeof ?softwareVersion;} {?softwareVersion ?label ?name;}" + "FILTER regex(?name, \"Oracle\", \"i\") }";
				
			String swQuerySelect = "SELECT DISTINCT ?system ?softwareModule WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareModule> ;} " +
			"{?softwareVersion <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareVersion> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consists>;} " +
			"{?typeof <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/SoftwareModuleForSoftwareVersion>;} " +
			"{?system ?has ?softwareModule;} {?softwareModule ?typeof ?softwareVersion;} {?softwareVersion ?label ?name;}" + "FILTER regex(?name, \"Oracle\", \"i\") }";
			
			String swSystemQuerySelect = "SELECT DISTINCT ?system ?softwareModule WHERE { {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareModule> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consists>;} " +
			"{?system ?has ?softwareModule;} {?system <" +  RDFS.LABEL + "> ?name } FILTER regex(?name, \"DMLSS\", \"i\") }";
			//icdDataQuery = URLEncoder.encode(icdDataQuery, "UTF-8");//(icdDataQuery);
			String swSystemQuerySelect2 = "SELECT DISTINCT ?system ?softwareModule WHERE { " +
					"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;} " +
			"{?softwareModule <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/SoftwareModule> ;} " +
			"{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consists>;} " +
			"{?system <" +  RDFS.LABEL + "> \"DMLSS\" }" +
			"{?system ?has ?softwareModule;} }";
			
			String neighborICDHoodQuery = "CONSTRUCT {?focus ?predicate ?object. ?subject ?predicate2 ?focus} WHERE {" +
					"{?focus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" +
					"{?focus2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" + 
					"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" + 
					"{?predicate2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consume> ;}" + 
					"{?focus <" +  RDFS.LABEL + "> ?name ;}" + 
					"{?focus ?predicate ?object}" + 
					"{?subject ?predicate2 ?focus}" +
					"FILTER (?name in (\"DMLSS\"))" +
					"}";
			
			String neighborICDHoodQuery2 = "SELECT  DISTINCT ?focus ?predicate ?object WHERE {" +
			"{?focus <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" +
			"{?focus2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" +
			"{?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject> ;}" +
			"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" + 
			"{?predicate2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Consume> ;}" + 
			"{?focus <" +  RDFS.LABEL + "> ?name ;}" + 
			"{?focus ?predicate ?object}" + 
			//"{?subject ?predicate2 ?focus}" +
			"FILTER (?name in (\"ABTS\"))" +
			"}";

			String capReq = "CONSTRUCT {?cap ?help ?req. ?cap ?label ?name} WHERE " +
					"{ {?req <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/Requirement>;} " +
					"{?help <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/FulFill>;} " +
					"{?cap <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/Capability>;} " +
					"{?cap ?help ?req;} {?bp <http://www.w3.org/2000/01/rdf-schema#label> ?name;} " +
					"Filter (?name in (\"Laboratory\"))}";
			
			String serviceData = "CONSTRUCT {?service ?provide ?data} WHERE " +
					"{ {?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/Service>}" +
					" {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Exposes>} " +
					" {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject>}" +
					"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" +
					"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Upstream> ;}" +
					"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
					"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Payload>}" +
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
			"{?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/Service>}" +
			" {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Exposes>} " +
			" {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject>}" +
			"{?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System> ;}" +
			"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
			"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
			"{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Payload>}" +
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
											"{?businessprocess <" + RDFS.LABEL + "> " +	"<http://health.mil/ontologies/dbcm/Concept/BusinessProcess/>;} " +
									 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
									 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
			"{?system1 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?upstream <"+RDFS.SUBPROPERTYOF + "> <http://health.mil/ontologies/dbcm/Relation/Provide> ;}" +
	 		"{?icd <"+RDF.TYPE + "> <http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;}" +
	 		"{?system1 <" + RDFS.LABEL + "> \"CHCS\"; }" +
	 		"{?downstream <"+RDFS.SUBPROPERTYOF +"> <http://health.mil/ontologies/dbcm/Relation/Consume>;}" +
	 		"{?system2 <" + RDF.TYPE + "> " +	"<http://health.mil/ontologies/dbcm/Concept/System>;} " +
	 		"{?system2 <" + RDFS.LABEL + "> \"DEERS\";}"+
	 		"{?system1 ?upstream ?icd ;}" +
	 		"{?icd ?downstream ?system2 .}" +
	 		"}";
			*/
			
			String bindQ = "SELECT  DISTINCT ?predicate WHERE " +
					"{?subject ?predicate ?object. " +
					"BIND (<http://health.mil/ontologies/dbcm/Concept/System/ABTS>" +
					" AS ?subject).}";

			String bindQ4 = "SELECT  ?subject ?predicate ?object WHERE " +
			"{{?subject ?predicate ?object.} " +
			"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation/Provide>;}"+
			"BIND (<http://health.mil/ontologies/dbcm/Concept/System/ABTS>" +
			" AS ?subject).}";

			String bindQ2 = "SELECT ?upstream   WHERE { BIND (<http://health.mil/ontologies/dbcm/Concept/System/ABTS>  AS ?sys ). " +
					"{?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument> ;} " +
					"{?upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http://health.mil/ontologies/dbcm/Relation/Provide>;} {?sys ?upstream ?icd .} }";
			
			String bindQ3 = "SELECT  ?predicate WHERE { " +
					"{?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					"<http:/health.mil/ontologies/dbcm/Relation> ;} " +
					//"BIND (<http://health.mil/ontologies/dbcm/Concept/System/CHCS> AS ?subject). " +
					"}";
			
			String bindQ5 = "SELECT  ?subject ?predicate ?object WHERE {{?subject ?predicate ?object.} {?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation>.} BIND (<http://health.mil/ontologies/dbcm/Concept/System/ABTS> AS ?subject).}";
			String bindQ6 = "CONSTRUCT  {?subject ?predicate ?object} WHERE {{?object  <http://www.w3.org/2000/01/rdf-schema#label> \"CRUD Procedure Order\";} {?subject <http://www.w3.org/2000/01/rdf-schema#label> \"ABTS-CHCS-Order Information\";} {?predicate <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation>;} {?subject ?predicate ?object.} }";
			
			String bindQ7 = "SELECT  ?subject ?predicate ?object WHERE {BIND (<http://health.mil/ontologies/dbcm/Concept/Service/AlertOnAbnormalValue> AS ?subject). ?subject ?predicate ?object;}";
			
			String tommyT = "INSERT {<http://health.mil/ontologies/dbcm/Concept/Service/AlertOnAbnormalValue><http://health.mil/ontologies/dbcm/Relation/Exposes><http://health.mil/ontologies/dbcm/Concept/DataObject/tom1>} WHERE{}";
			String tommyT2 = "INSERT { <http://health.mil/ontologies/dbcm/Concept/DataObject/tom1><http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/DataObject>}WHERE{}"; 

			//System.out.println(" Query >>> \n" + serviceData2);
			//Forest f = tester.convertSailtoGraph(res);
			//tester.painGraph(f);
			tester.getCoreData(null);
			String [] names = {"focus"}; //,"predicate", "object"};
			//tester.doRemoveSelect(bindQ7,names);
			tester.execInsertQuery(tommyT2);
			tester.closeGraph();
			tester.testTemp();

			
			//SELECT  DISTINCT ?predicate WHERE {BIND (<http://health.mil/ontologies/dbcm/Concept/InterfaceControlDocument/ABTS-CHCS-Patient ID> AS ?subject). {?subject ?predicate ?object.}}
		} catch (Exception ignored) {
			System.err.println("Exception " + ignored);
			ignored.printStackTrace();
		}

	}
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException {

		try {
			Update up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			//sc.addStatement(vf.createURI("<http://health.mil/ontologies/dbcm/Concept/Service/tom2>"),vf.createURI("<http://health.mil/ontologies/dbcm/Relation/Exposes>"),vf.createURI("<http://health.mil/ontologies/dbcm/Concept/BusinessLogicUnit/tom1>"));
			System.out.println("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			rc.setAutoCommit(false);
			up.execute();
			//rc.commit();
	        InferenceEngine ie = ((BigdataSail)bdSail).getInferenceEngine();
	        ie.computeClosure(null);
			sc.commit();
			
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
