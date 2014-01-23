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

import java.io.File;

import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;



/**
 */
public class RDFSesameFileReader {

	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String [] args) throws Exception
	{
		String workingDir = System.getProperty("user.dir");
		String fileName = workingDir + "/db/foaf.xml";
	
		Repository myRepository = new SailRepository(
                new ForwardChainingRDFSInferencer(
                new MemoryStore()));
		myRepository.initialize();
		
		File file = new File(fileName);
		String baseURI = "http://example.org/example/local";
		RepositoryConnection con = myRepository.getConnection();
		
	    con.add(file, null, RDFFormat.RDFXML);
	
		String query = "CONSTRUCT {?subject ?predicate ?object} WHERE {" +
		//"{?subject <" +  RDFS. + "> <http://sandbox-api.smartplatforms.org/records/2169591> ;}" +
		//"BIND(<http://sandbox-api.smartplatforms.org/records/2169591>  AS ?subject )." +
		"{?subject ?predicate ?object.}" +
		//"BIND (<http://sandbox-api.smartplatforms.org/records/2169591> AS ?subject)." +
		"}";

		GraphQuery sagq = con.prepareGraphQuery(QueryLanguage.SPARQL,
				query);
		GraphQueryResult res = sagq.evaluate();
		while(res.hasNext())
		{
			Statement stmt = res.next();
			System.out.println(">>> " + stmt.getSubject()+ " <> " + stmt.getPredicate() + "<>" + stmt.getObject());
		}	
		
		/*
		 * SELECT DISTINCT ?predicate  WHERE {{?subject ?predicate ?object.}
{?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>.}
}
		 */
		
		String query2 = "SELECT  DISTINCT ?predicate WHERE {" +
		//"{?subject <" +  RDFS. + "> <http://sandbox-api.smartplatforms.org/records/2169591> ;}" +
		//"BIND(<http://sandbox-api.smartplatforms.org/records/2169591>  AS ?subject )." +
		"?subject ?predicate ?object." +
		
		"BIND (<http://sandbox-api.smartplatforms.org/records/2169591> AS ?subject)." +
		"}";
		
		String query3 = "SELECT  ?subject ?predicate ?object WHERE {?subject ?predicate ?object. " +
				"BIND (<http://sandbox-api.smartplatforms.org/records/2169591/allergies/873252> AS ?subject). " +
				"BIND (<http://purl.org/dc/terms/title> AS ?predicate).}";
		
		String query4 = "SELECT  ?subject ?predicate ?object WHERE " +
				"{?subject ?predicate ?object. " +
				"BIND (<_:node17gjauj84x5> AS ?subject). " +
				"BIND (<http://smartplatforms.org/terms#drugAllergen> AS ?predicate).}";
		
		String query5 = "SELECT ?relation ?domain ?range WHERE {" +
				"{?relation <" + RDF.TYPE + "> <http://www.w3.org/2002/07/owl#ObjectProperty>;}" +
						"{?relation <" + RDFS.DOMAIN + "> ?domain;}" +
						"{?relation <" + RDFS.RANGE + "> ?range;}" +
						"}";
				
		
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, query5);
		System.out.println("\nSPARQL: " + query3);
		tq.setIncludeInferred(true /* includeInferred */);
		TupleQueryResult sparqlResults = tq.evaluate();

		while(sparqlResults.hasNext())
		{
			BindingSet bs = sparqlResults.next();
			System.out.println(" DOMAIN >>> " + bs.getBinding("domain") + "  " + bs.getBinding("relation") + "  Range >>> " + bs.getBinding("range")); // + "<<>>" + bs.getBinding("predicate")+"<<>>" + bs.getBinding("object"));
		}

	}
}
