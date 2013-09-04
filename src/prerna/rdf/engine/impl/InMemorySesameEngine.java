package prerna.rdf.engine.impl;

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
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class InMemorySesameEngine extends AbstractEngine implements IEngine {

	Properties bdProp = null;
	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	boolean connected = false;

	public void setRepositoryConnection(RepositoryConnection rc)
	{
		this.rc = rc;
	}
	
	@Override
	public void openDB(String propFile)
	{
		// no meaning to this now
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
