package prerna.rdf.main;


import il.ac.technion.cs.d2rqUpdate.ModelD2RQUpdate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;



public class D2RQTester {
	
	Model d2rqModel = null;
	
	public void openMapping()
	{
		
	}
	
	public static void main(String [] args)
	{
		// Set up the ModelD2RQ using a mapping file
		String workingDir = System.getProperty("user.dir");		
		String propFile = workingDir + "/mapping-iswc2.ttl";
		
		String pckg = "de.fuberlin.wiwiss.d2rq.";
		Logger rdqlLogger = Logger.getLogger(pckg + "RDQL");
		
		org.apache.log4j.BasicConfigurator.configure();
		
		rdqlLogger.setLevel(Level.DEBUG);

		Model m = new de.fuberlin.wiwiss.d2rq.ModelD2RQ("file:" + propFile);
		
		String sparql = "CONSTRUCT {?s ?p ?o.} WHERE {?s ?p ?o.}";
		Query q = QueryFactory.create(sparql); 
		//ResultSet rs = QueryExecutionFactory.create(q, m).execSelect();
		Model m2 = QueryExecutionFactory.create(q, m).execConstruct();
		/*while (rs.hasNext()) {
		    QuerySolution row = rs.nextSolution();
		    System.out.println("Got here ");
		    //System.out.println(row.)
		};*/
		StmtIterator stmti = m2.listStatements();
		while(stmti.hasNext())
		{
			Statement stmt = stmti.next();
			System.out.println(stmt.getSubject()+ "<<>>" + stmt.getPredicate() + "<<>>" + stmt.getObject());
		}
		
		//String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Yolanda\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		//String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#phone> ?o}";
		/*Query q2 = QueryFactory.create(sparql); 
		ResultSet rs = QueryExecutionFactory.create(q2, m).execSelect();
		while (rs.hasNext()) {
		    QuerySolution row = rs.nextSolution();
		    
		    System.out.println("-->" + row.get("s") + "<<>>" + row.get("p") + "<<>>" +row.get("o"));
		    
		    //System.out.println("Title: " + row.getLiteral("paperTitle").getString());
		    //System.out.println("Author: " + row.getLiteral("authorName").getString());
		};*/
		
		// Trying sparql insert
		System.out.println("Trying insert ");
		
		Model m3 = new ModelD2RQUpdate("file:" + propFile);

		
		String sparql2 = "DELETE {?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>} WHERE { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Yolanda\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		String sparql4 = "INSERT {<http://conferences.org/comp/confno#90> <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		String sparql3 = "MODIFY DELETE " + "{?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>}" +
			"INSERT { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"667\"^^<http://www.w3.org/2001/XMLSchema#string> }" + 
			"WHERE { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Varun\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		
		UpdateRequest u = UpdateFactory.create(sparql4);
		UpdateAction.execute(u, m2);
		System.out.println("Done Inserts ");
		
		m2.close();
		m.close();
	}
	
	

}
