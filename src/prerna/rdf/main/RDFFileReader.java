package prerna.rdf.main;

import java.io.InputStream;

import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;

public class RDFFileReader {

	public static void main(String [] args)
	{
		String workingDir = System.getProperty("user.dir");
		String fileName = workingDir + "/Sample3.rdf";

		System.out.println("Completed loading file ");
		InputStream in = FileManager.get().open(fileName); 			
		System.out.println("Completed loading file ");
		Model jenaModel = ModelFactory.createDefaultModel();//  .read(in, null); //, rdfFileType);
		jenaModel.read(in, null, "RDF/XML");
		
		//com.hp.hpl.jena.query.Query q2 = QueryFactory.create(" ?s ?p ?o WHERE {?s ?p ?o}"); 
		//com.hp.hpl.jena.query.ResultSet rs = QueryExecutionFactory.create(q2, jenaModel).execSelect();
		
		StmtIterator sti = jenaModel.listStatements();
		System.out.println("Iterating " + jenaModel.isEmpty());
	
		String query = "SELECT ?subject ?predicate ?object WHERE {" +
				//"{?subject <" +  RDFS. + "> <http://sandbox-api.smartplatforms.org/records/2169591> ;}" +
				//"BIND(<http://sandbox-api.smartplatforms.org/records/2169591>  AS ?subject )." +
				"?subject ?predicate ?object." +
				"BIND <http://sandbox-api.smartplatforms.org/records/2169591> AS ?subject." +
				"}";
		
		//com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		com.hp.hpl.jena.query.ResultSet rs = QueryExecutionFactory.create(query, jenaModel).execSelect();

		/*QueryExecution qexec = QueryExecutionFactory.create(queryVar, jenaModel) ;
		Model resultModel = qexec.execConstruct() ;
		sti = resultModel.listStatements();

		
		while(sti.hasNext())
		{
			Statement stmt = sti.next();
			System.out.println("Statement " + stmt.getSubject() + "<>" + stmt.getPredicate() + "<>" + stmt.getObject());
		}*/
		while(rs.hasNext())
		{
			QuerySolution soln = rs.next();
			System.out.println(soln.get("?subject"));
		}
		
	}
	
}
