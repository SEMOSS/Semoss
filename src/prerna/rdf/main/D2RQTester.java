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


import il.ac.technion.cs.d2rqUpdate.ModelD2RQUpdate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;

import de.fuberlin.wiwiss.d2rq.jena.ModelD2RQ;

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
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);		
		String propFile = workingDir + "/mapping-iswc2.ttl";
		
		String pckg = "de.fuberlin.wiwiss.d2rq.";
		Logger rdqlLogger = Logger.getLogger(pckg + "RDQL");
		
		org.apache.log4j.BasicConfigurator.configure();
		
		rdqlLogger.setLevel(Level.DEBUG);

		Model m = new ModelD2RQ("file:" + propFile);
		
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
		
//		String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#FirstName> \"Yolanda\"^^<http://www.w3.org/2001/XMLSchema#string>}";
//		String sparql = "SELECT ?s ?p ?o WHERE {?s <http://annotation.semanticweb.org/iswc/iswc.daml#phone> ?o}";
		Query q2 = QueryFactory.create(sparql); 
		ResultSet rs = QueryExecutionFactory.create(q2, m).execSelect();
		while (rs.hasNext()) {
		    QuerySolution row = rs.nextSolution();
		    
		    rdqlLogger.debug("-->" + row.get("s") + "<<>>" + row.get("p") + "<<>>" +row.get("o"));
		    
		    rdqlLogger.debug("Title: " + row.getLiteral("paperTitle").getString());
		    rdqlLogger.debug("Author: " + row.getLiteral("authorName").getString());
		};
		
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
