package prerna.rdf.main;

import java.io.File;

import org.openrdf.model.Statement;
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



public class RDFSesameFileReader {

	public static void main(String [] args) throws Exception
	{
		String workingDir = System.getProperty("user.dir");
		String fileName = workingDir + "/Sample3.rdf";
	
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
		"BIND (<http://sandbox-api.smartplatforms.org/records/2169591> AS ?subject)." +
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
{?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>.}
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
		
		TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, query4);
		System.out.println("\nSPARQL: " + query3);
		tq.setIncludeInferred(true /* includeInferred */);
		TupleQueryResult sparqlResults = tq.evaluate();

		while(sparqlResults.hasNext())
		{
			BindingSet bs = sparqlResults.next();
			System.out.println("Predicate >>> " + bs.getBinding("subject") + "<<>>" + bs.getBinding("predicate")+"<<>>" + bs.getBinding("object"));
		}

	}
}
