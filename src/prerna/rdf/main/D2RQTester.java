/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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

/**
 */
public class D2RQTester {
	
	Model d2rqModel = null;
	
	/**
	 * Method openMapping.
	 */
	public void openMapping()
	{
		
	}
	
	/**
	 * Method main.
	 * @param args String[]
	 */
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
		    rdqlLogger.debug("Got here ");
		    //rdqlLogger.debug(row.)
		};*/
		StmtIterator stmti = m2.listStatements();
		while(stmti.hasNext())
		{
			Statement stmt = stmti.next();
			rdqlLogger.info(stmt.getSubject()+ "<<>>" + stmt.getPredicate() + "<<>>" + stmt.getObject());
		}
		
		//String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Yolanda\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		//String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#phone> ?o}";
		/*Query q2 = QueryFactory.create(sparql); 
		ResultSet rs = QueryExecutionFactory.create(q2, m).execSelect();
		while (rs.hasNext()) {
		    QuerySolution row = rs.nextSolution();
		    
		    rdqlLogger.debug("-->" + row.get("s") + "<<>>" + row.get("p") + "<<>>" +row.get("o"));
		    
		    //rdqlLogger.debug("Title: " + row.getLiteral("paperTitle").getString());
		    //rdqlLogger.debug("Author: " + row.getLiteral("authorName").getString());
		};*/
		
		// Trying sparql insert
		rdqlLogger.debug("Trying insert ");
		
		Model m3 = new ModelD2RQUpdate("file:" + propFile);
		
		String sparql2 = "DELETE {?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>} WHERE { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Yolanda\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		String sparql4 = "INSERT {<http://conferences.org/comp/confno#90> <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		String sparql3 = "MODIFY DELETE " + "{?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"666\"^^<http://www.w3.org/2001/XMLSchema#string>}" +
			"INSERT { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#phone> \"667\"^^<http://www.w3.org/2001/XMLSchema#string> }" + 
			"WHERE { ?person <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Varun\"^^<http://www.w3.org/2001/XMLSchema#string>}";
		
		UpdateRequest u = UpdateFactory.create(sparql4);
		UpdateAction.execute(u, m2);
		rdqlLogger.debug("Done Inserts ");
		
		m2.close();
		m.close();
	}
}
