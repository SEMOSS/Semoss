package prerna.rdf.engine.impl;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class RDFFileSesameEngine extends AbstractEngine implements IEngine {

	Properties bdProp = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	boolean connected = false;

	
	@Override
	public void openDB(String propFile)
	{
		try
		{
			bdProp = loadProp(propFile);
			// return g;
			String workingDir = System.getProperty("user.dir");

			Repository myRepository = new SailRepository(
	                new ForwardChainingRDFSInferencer(
	                new MemoryStore()));
			myRepository.initialize();
			
			String fileName = bdProp.getProperty(Constants.RDF_FILE_NAME);
			String rdfFileType = bdProp.getProperty(Constants.RDF_FILE_TYPE);
			String baseURI = bdProp.getProperty(Constants.RDF_FILE_BASE_URI);

			File file = new File(workingDir + "/" + fileName);
			rc = myRepository.getConnection();
			
			if(rdfFileType.equalsIgnoreCase("RDF/XML")) rc.add(file, baseURI, RDFFormat.RDFXML);
			else if(rdfFileType.equalsIgnoreCase("TURTLE")) rc.add(file, baseURI, RDFFormat.TURTLE);
			else if(rdfFileType.equalsIgnoreCase("BINARY")) rc.add(file, baseURI, RDFFormat.BINARY);
			else if(rdfFileType.equalsIgnoreCase("N3")) rc.add(file, baseURI, RDFFormat.N3);
			else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) rc.add(file, baseURI, RDFFormat.NTRIPLES);
			else if(rdfFileType.equalsIgnoreCase("TRIG")) rc.add(file, baseURI, RDFFormat.TRIG);
			else if(rdfFileType.equalsIgnoreCase("TRIX")) rc.add(file, baseURI, RDFFormat.TRIX);

		    this.connected = true;
		}catch(Exception ignored)
		{
			this.connected = false;
			ignored.printStackTrace();
		}
	}
	
	private Properties loadProp(String fileName) throws Exception
	{
		Properties retProp = new Properties();
		retProp.load(new FileInputStream(fileName));
		System.out.println("Properties >>>>>>>>" + bdProp);
		return retProp;
	}

	@Override
	public void closeDB() {
		// TODO Auto-generated method stub
		// ng.stopTransaction(Conclusion.SUCCESS);
		try {
			rc.close();
			connected = false;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// ng.shutdown();
	}

	@Override
	public GraphQueryResult execGraphQuery(String query) {
		 GraphQueryResult res = null;
		try {
			GraphQuery sagq = rc.prepareGraphQuery(QueryLanguage.SPARQL,
						query);
				res = sagq.evaluate();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;	
	}

	@Override
	public TupleQueryResult execSelectQuery(String query) {
		
		TupleQueryResult sparqlResults = null;
		
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, query);
			System.out.println("\nSPARQL: " + query);
			tq.setIncludeInferred(true /* includeInferred */);
			sparqlResults = tq.evaluate();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sparqlResults;
	}

	@Override
	public void execInsertQuery(String query) {
		// TODO Auto-generated method stub

	}
	
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.SESAME;
	}
	
	public Vector<String> getEntityOfType(String sparqlQuery)
	{
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
			System.out.println("\nSPARQL: " + sparqlQuery);
			tq.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult sparqlResults = tq.evaluate();
			Vector<String> strVector = new Vector();
			while(sparqlResults.hasNext())
				strVector.add(Utility.getInstanceName(sparqlResults.next().getValue(Constants.ENTITY)+ ""));

			return strVector;
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean isConnected()
	{
		return connected;
	}

	@Override
	public Boolean execAskQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}
}
