package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLConnection;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.config.SPARQLRepositoryConfig;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;

public class RemoteSparqlEngine extends AbstractEngine implements IEngine {

	BigdataSail bdSail = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	boolean connected = false;

	
	@Override
	public void openDB(String propFile)
	{
		try
		{
			super.openDB(propFile);
			String sparqlQEndpoint = prop.getProperty(Constants.SPARQL_QUERY_ENDPOINT);
			String sparqlUEndpoint = prop.getProperty(Constants.SPARQL_UPDATE_ENDPOINT);

			
			SPARQLRepository repo = new SPARQLRepository(sparqlQEndpoint);
			Hashtable <String, String> myMap = new Hashtable<String,String>();
			myMap.put("apikey","d0184dd3-fb6b-4228-9302-1c6e62b01465");
			repo.setAdditionalHttpHeaders(myMap);
			rc = repo.getConnection();//new SPARQLConnection(repo, sparqlQEndpoint, sparqlUEndpoint);
			
	
			// new ForwardChainingRDFSInferencer(bdSail);
			//InferenceEngine ie = bdSail.getInferenceEngine();
	
			// System.out.println("ie forward chaining " + ie);
			// need to convert to constants
			String dbcmFile = prop.getProperty(Constants.DBCM_Prop);
			String workingDir = System.getProperty("user.dir");
			
			dbcmFile = workingDir + "/" + dbcmFile;
			rdfMap = DIHelper.getInstance().getCoreProp();
			
			this.connected = true;
			// return g;
		}catch(Exception ignored)
		{
			ignored.printStackTrace();
		}
	}
	
	@Override
	public void closeDB() {
		// TODO Auto-generated method stub
		// ng.stopTransaction(Conclusion.SUCCESS);
		try {
			bdSail.shutDown();
			connected = false;
		} catch (SailException e) {
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
			//tq.setIncludeInferred(true /* includeInferred */);
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
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException, RepositoryException, MalformedQueryException {

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
		rc.commit();
		

	}
	@Override
	public Boolean execAskQuery(String query) {
		
		BooleanQuery bq;
		boolean response = false;
		try {
			bq = rc.prepareBooleanQuery(QueryLanguage.SPARQL, query);
			System.out.println("\nSPARQL: " + query);
			response = bq.evaluate();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return response;
		

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
}
