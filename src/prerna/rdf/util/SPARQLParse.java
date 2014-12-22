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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Coalesce;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.GraphPattern;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SPARQLParse {

	List<StatementPattern> patterns = null;
	Hashtable sourceTarget = null;
	Hashtable constantHash = null;
	public RepositoryConnection rc = null;
	SailConnection sc = null;
	ValueFactory vf = null;
	IEngine engine = null;
	IEngine bdEngine = null;

	public static void main(String[] args) throws Exception {
		String query2 = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) (COALESCE(?status, \"\") AS ?SystemStatus) WHERE {BIND(<http://health.mil/ontologies/Concept/SystemCategory/Central> AS ?SystemCategory) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has>;}{?System ?Has ?SystemCategory}OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/Status> ?status } } } LIMIT 1";
		
		String query = "SELECT ?Capability ?support ?BusinessProcess WHERE {BIND(<http://health.mil/ontologies/Concept/Capability/Laboratory> AS ?Capability) {?support <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?Capability ?support ?BusinessProcess;} }";

		String query1 = "SELECT ?Capability ?support ?BusinessProcess WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;} {?support <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?Capability ?support ?BusinessProcess;} }";
		
		//String query = "CONSTRUCT {?System1 ?Upstream ?ICD. ?ICD ?Downstream ?System2. ?ICD ?carries ?Data1. ?ICD ?contains2 ?prop2. ?System3 ?Upstream2 ?ICD2. ?ICD2 ?contains1 ?prop. ?ICD2 ?Downstream2 ?System1.?ICD2 ?carries2 ?Data2.?System1 ?Provide ?BLU}" ;
			String query3 = "SELECT ?System1 ?Upstream ?ICD ?Downstream ?System2 ?carries ?Data1 ?contains2 ?prop2 ?System3 ?Upstream2 ?ICD2 ?contains1 ?prop ?Downstream2 ?carries2 ?Data2 ?Provide ?BLU" +
				" WHERE { {?System1  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
		
		// String query =
		// "SELECT ?db ?contains ?prop WHERE { {?db <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Database> ;} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?db ?contains ?prop ;} } LIMIT 2";

		query = "SELECT DISTINCT ?Director (AVG(?Title__MovieBudget) AS ?Title__MovieBudget) WHERE { BIND(<@Studio-http://semoss.org/ontologies/Concept/Studio@> AS ?Studio) {?Title &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Title&gt;} {?Director &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Director&gt;} {?Studio &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Studio&gt;} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedBy&gt; ?Director} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedAt&gt; ?Studio} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/MovieBudget&gt; ?Title__MovieBudget} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-International&gt; ?Title__Revenue_International} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic&gt; ?Title__Revenue_Domestic} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience&gt; ?Title__RottenTomatoes_Audience} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics&gt; ?Title__RottenTomatoes_Critics}  }  GROUP BY ?Director";	
		query = query.replace("&lt;", "<");
		query = query.replace("&gt;", ">");
			
		SPARQLParse parse = new SPARQLParse();

		String fileName = "C:/Users/bisutton/workspace/SEMOSS/db/TAP_Core_Data.smss";

		//parse.bdEngine = new BigDataEngine();

		//parse.bdEngine.openDB(fileName);
		//parse.createRepository(); // create in memory database
		parse.parseIt(query); // parse the query into grammar
		//parse.executeQuery(query, parse.bdEngine);
		// load base DB
		System.out.println("Loading base DB");
		//parse.loadBaseDB(parse.bdEngine.getProperty(Constants.OWL)); // load the OWL
		System.out.println("Testing Query"); 
		//parse.testQueryGen(); // test the generated query
		//parse.testIt(query1);
		//parse.exportToFile(); // export database to file if need to
		
	}
	
	public void executeQuery(String query, IEngine engine)
	{
		//Select logic goes here
		try {
			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setEngine(engine);
			wrapper.setQuery(query);
			wrapper.executeQuery();
			wrapper.getVariables();
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement stmt = wrapper.next();
				System.out.println("Binding " + stmt.rawPropHash);
				generateTriple(stmt.rawPropHash); // recreate the stuff in the memory including putting the OWL into it
			}
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//*/

		
	}

	public void exportToFile() {
		FileWriter fileWrite = null;
		try {
			String output = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + "output.xml";
			fileWrite = new FileWriter(output);
			rc.export(new RDFXMLPrettyWriter(fileWrite));
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try{
				if(fileWrite!=null)
					fileWrite.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Export complete");
	}

	public void loadBaseDB(String propFile) {
		FileInputStream fileIn = null;
		try {
			//Properties prop = new Properties();
			//prop.load(new FileInputStream(propFile));

			// get the owl file
			//String owler = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/"
			//		+ prop.get(Constants.OWL) + "";
			String owler = "C:/Users/bisutton/workspace/SEMOSS/db/TAP_Core_Data/TAP_Core_Data_OWL.OWL";
			fileIn = new FileInputStream(owler);
			rc.add(fileIn, "http://semoss.org",RDFFormat.RDFXML);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void testIt(String query) {
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setEngine(engine);
		wrapper.setQuery(query);
		wrapper.executeQuery();
		wrapper.getVariables();
		while (wrapper.hasNext()) {
			SesameJenaSelectStatement stmt = wrapper.next();
			System.out.println("Binding " + stmt.rawPropHash);
			// parse.generateTriple(stmt.rawPropHash);
		}

	}

	public void createRepository() {
		try {
			Repository myRepository2 = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository2.initialize();
			rc = myRepository2.getConnection();
			sc = ((SailRepositoryConnection) rc).getSailConnection();

			engine = new InMemorySesameEngine();
			((InMemorySesameEngine) engine).setRepositoryConnection(rc);
			vf = rc.getValueFactory();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Hashtable <String, Integer> parseIt(String query) {
		Hashtable <String, Integer> finalHash = new Hashtable<String, Integer>();
		try {
			SPARQLParser parser = new SPARQLParser();

			//System.out.println("Query is " + query);
			ParsedQuery query2 = parser.parseQuery(query, null);
			//System.out.println(">>>" + query2.getTupleExpr());
			StatementCollector collector = new StatementCollector();
			query2.getTupleExpr().visit(collector);

			patterns = collector.getPatterns();
			//System.err.println("Patterns is " + patterns);
			sourceTarget = collector.sourceTargetHash;
			//System.err.println("Source target is " + sourceTarget);
			constantHash = collector.constantHash;
			//System.out.println("Constants " + constantHash);
			finalHash = getURIList();
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		return finalHash;
	}
	
	public Hashtable getURIList()
	{
		Hashtable <String, Integer> finalHash = new Hashtable<String, Integer>();
		Hashtable <String, String> types = new Hashtable<String, String>();
		
		// get all the constants
		Iterator values = constantHash.values().iterator();
		while(values.hasNext())
		{
			String value = values.next() +"";
			finalHash.put(value,  new Integer(1));
		}
		
		for(int patIndex = 0;patIndex < patterns.size();patIndex++)
		{
			StatementPattern thisPattern = patterns.get(patIndex);
			// couple of things to check
			// is the subject or predicate a constant
			// if so add it
			
			Var subjectVar = thisPattern.getSubjectVar();
			Var objectVar = thisPattern.getObjectVar();
			//System.out.println(" " + subjectVar.getValue());
			finalHash = recordVar(subjectVar, finalHash);
			finalHash = recordVar(objectVar, finalHash);
			
			Var predicateVar = thisPattern.getPredicateVar();
			//System.err.println("Predicate var " + predicateVar.getValue());
			if(predicateVar.isConstant() && (predicateVar.getValue()+"").equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
			{
				//System.err.println("subject  " + subjectVar.getName());
				types.put(subjectVar.getName()+"_" + predicateVar.getValue(), objectVar.getValue()+"");
			}
		}
		
		// synchronize it
		Hashtable <String, Integer> synchronizedHash = new Hashtable<String, Integer>();
		Enumeration keys = finalHash.keys();
		while(keys.hasMoreElements())
		{
			String key = ""+keys.nextElement();
			if(key.contains(":")) // namespaced let it go
			{
				Integer typeProxyCount = finalHash.get(key);
				if(synchronizedHash.containsKey(key))
					typeProxyCount = typeProxyCount + synchronizedHash.get(key);
				synchronizedHash.put(key, typeProxyCount);
			}else
			{
				String typeName = types.get(key + "_http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
				Integer typeProxyCount = finalHash.get(key);
				if(typeName != null)
				{
					if(synchronizedHash.containsKey(typeName))
							typeProxyCount = typeProxyCount + synchronizedHash.get(typeName);
					synchronizedHash.put(typeName, typeProxyCount);
				}
			}
		}
		//System.out.println("Type has " + types);
		//System.out.println("Final Hash " + finalHash);
		//System.out.println("URI List " + synchronizedHash);
		return synchronizedHash;
	}
	
	public Hashtable <String, Integer> recordVar(Var var, Hashtable <String, Integer> inputHash)
	{
		//System.out.println("Var is " + var.getValue());
		if(var.hasValue())
		{
			Integer count = inputHash.get(var.getValue()+"");
			if(count == null)
				count = new Integer(0);
			count++;
			inputHash.put(var.getValue()+"", count);
		}
		else
		{
			Integer count = inputHash.get(var.getName()+"");
			if(count == null)
				count = new Integer(0);
			count++;
			inputHash.put(var.getName()+"", count);
		}
		return inputHash;
		
	}
	

	public void generateTriple(Hashtable binds) throws SailException {
		for (int index = 0; index < patterns.size(); index++) {
			StatementPattern pattern = patterns.get(index);
			//System.out.println("-----------");
			createTriple(pattern, binds, engine);
		}
	}

	// binds tells me all the current bindings
	public void createTriple(StatementPattern pattern, Hashtable binds, IEngine rc)
			throws SailException {
		// get the pattern to get the subject, predicate and object
		Object subject, predicate, object;
		// get the subject
		// typically if the value is available then it should be the value
		// else it is the name of the pattern

		subject = pattern.getSubjectVar().getValue();
		if (subject == null)
			subject = pattern.getSubjectVar().getName();
		// System.out.println(pattern.getSubjectVar().getName() + "<>" +
		// pattern.getSubjectVar().getValue() );
		// if(pattern.getSubjectVar().getValue() == null)
		// System.out.println(subject.getClass());

		// predicate
		predicate = pattern.getPredicateVar().getValue();
		if (predicate == null)
			predicate = pattern.getPredicateVar().getName();
		// System.out.println(pattern.getPredicateVar().getName() + "<>"
		// +pattern.getPredicateVar().getValue() + "<>" +
		// pattern.getPredicateVar().isAnonymous());
		// System.out.println(predicate.getClass());

		object = pattern.getObjectVar().getValue();
		if (object == null)
			object = pattern.getObjectVar().getName();
		// System.out.println(pattern.getObjectVar().getName()+ "<>" +
		// pattern.getObjectVar().getValue());
		// System.out.println(object.getClass());

		// now I need to check to see if the subject, predicate or object
		Object subjectT = returnBinding(subject + "", binds);
		Object predicateT = returnBinding(predicate + "", binds);
		Object objectT = returnBinding(object + "", binds);
		//System.err.println(" Before " + subject + "<>" + predicate + "<>" + object);
		//System.err.println(" After " + subjectT + "<>" + predicateT + "<>" + objectT);
		if(subjectT != null && predicateT != null && objectT != null)
		{
			if (!(subjectT.toString().startsWith("http://")))
				subjectT = new URIImpl("semoss:" + subjectT);
			if (!(predicateT.toString().startsWith("http://")))
				predicateT = "semoss:" + predicateT;
			if (objectT.toString().startsWith("http://"))
				objectT = new URIImpl(objectT + "");
			else if(objectT.toString().equalsIgnoreCase(object + ""))
				objectT = new URIImpl("semoss:" + objectT);
	
			// if(objectT instanceof String)
			// objectT = new U
	
			/*System.out.println("TRIPLE " + subjectT + "<>" + predicateT + "<>"
					+ objectT + "<>" + objectT.getClass()
					+ (objectT instanceof Literal));
	*/
			// Statement stmt = new StatementImpl(vf.createURI(subjectT+""),
			// vf.createURI(predicateT+""), (Value) objectT);
			// rc.
			// sc.addStatement(vf.createURI((String) subjectT),
			// vf.createURI((String) predicateT), (Value)objectT);
			rc.addStatement(subjectT + "", predicateT + "", objectT, true);
		}
	}

	public Object returnBinding(String source, Hashtable binding) {
		if (sourceTarget.containsKey(source)) {
			String target = (String) sourceTarget.get(source);
			// get the value from binding
			if (binding.containsKey(target))
				return binding.get(target);
			else if (constantHash.containsKey(source)) {
				return constantHash.get(source);
			}
		}
		return source;
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
			Var h = new Var("w");
			Var bvType = new Var("bvType", vf.createURI("http://semoss.org/ontologies/Relation/Contains/BusinessValue"));

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
			//bs2.addBinding("p", vf.createURI("http://semoss.org/ontologies/Concept/System")); // binding 1
			bs2.addBinding("t", vf.createURI("http://semoss.org/ontologies/Concept/System")); // binding 2
			//bs2.addBinding("bvType", vf.createURI("http://semoss.org/ontologies/Relation/Contains/BusinessValue")); // binding 2
			bsa.setBindingSets(vbs);
			gp.addRequiredTE(bsa);
					
			// adding math portions
			ValueConstant arg1 = new ValueConstant(vf.createLiteral(2.0));
			ValueConstant arg2 = new ValueConstant(vf.createLiteral(4.0));
			MathExpr mathExpr = new MathExpr(p, arg2, MathExpr.MathOp.PLUS);

			Var m = new Var("m");
			// adding coalesce
			Coalesce c = new Coalesce();
			c.addArgument(m);
			//c.addArgument(new ValueConstant(vf.createURI("http://semoss.org")));
			c.addArgument(mathExpr);
			
			ExtensionElem cee = new ExtensionElem(c,"m");
			Extension ce = new Extension(new GraphPattern().buildTupleExpr());
			ce.addElement(cee);
			gp.addRequiredTE(ce);

			// adding triples
			gp.addRequiredSP(systemName, w, t);
			gp.addRequiredSP(systemName, bvType, p);

			System.out.println(gp.buildTupleExpr());

			Projection proj = new Projection(new GraphPattern().buildTupleExpr());
			ProjectionElemList list = new ProjectionElemList();
			list.addElements(new ProjectionElem("x","k"), new ProjectionElem("t"), new ProjectionElem("p"), new ProjectionElem("m"));
			proj.setProjectionElemList(list);
			gp.addRequiredTE(proj);
			//System.out.println(gp.buildTupleExpr());
			
			//gp.addRequiredTE()

			/*gp.addRequiredTE(new Projection(gp.buildTupleExpr(),
					new ProjectionElemList(new ProjectionElem("x")
					,new ProjectionElem("t"))));
	*/
			//System.out.println(gp.buildTupleExpr());
			//
			
			
			//gp.
			
			TupleExpr query2 = gp.buildTupleExpr(); /*new Projection(gp.buildTupleExpr(),
					new ProjectionElemList(new ProjectionElem("x")
					,new ProjectionElem("t")
					// new ProjectionElem("y")
					));*/
			
			
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
					//(SailRepositoryConnection) ((BigDataEngine)bdEngine).rc);
			
			System.out.println("\nSPARQL: " + query3);

			TupleQueryResult sparqlResults = q.evaluate();
			//tq.setIncludeInferred(true /* includeInferred */);
			//TupleQueryResult sparqlResults = tq.evaluate();

			System.out.println("Output is " );
			while (sparqlResults.hasNext()) {
				BindingSet bs = sparqlResults.next();
				System.out.println("Predicate >>> " + bs.getBinding("x") + "  >>> " + bs.getBinding("t") + " >>> " + bs.getBinding("m"));
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

class MyTupleQuery extends SailTupleQuery {
	public MyTupleQuery(ParsedTupleQuery query, SailRepositoryConnection sc) {
		super(query, sc);

	}
}

class MyExtension extends Extension
{
	@Override
	public Set<String> getBindingNames()
	{
		System.out.println("Going to crash");
		return super.getBindingNames();
	}
}
