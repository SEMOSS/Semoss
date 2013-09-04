package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.util.FileManager;

public class RemoteJenaEngine extends AbstractEngine implements IEngine {
	
	Model jenaModel = null;
	Logger logger = Logger.getLogger(getClass());
	String propFile = null;
	String serviceURI = null;
	boolean connected = false;

	@Override
	public void closeDB() {
		jenaModel.close();
		logger.info("Closing the database to the file " + propFile);		
	}

	@Override
	public Object execGraphQuery(String query) {
	
		com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		
		QueryEngineHTTP qexec = QueryExecutionFactory.createServiceRequest(this.serviceURI, queryVar);
		String params = prop.getProperty(Constants.URL_PARAM);
		StringTokenizer paramTokens = new StringTokenizer(params, ";");
		while(paramTokens.hasMoreTokens())
		{
			String token = paramTokens.nextToken();
			qexec.addParam(token, prop.getProperty(token));			
		}
		Model resultModel = qexec.execConstruct() ;
		logger.info("Executing the RDF File Graph Query " + query);
		return resultModel;
		//qexec.close() ;
	}

	@Override
	public Object execSelectQuery(String query) {
		
		com.hp.hpl.jena.query.Query q2 = QueryFactory.create(query); 
		com.hp.hpl.jena.query.Query queryVar = QueryFactory.create(query) ;
		
		QueryEngineHTTP qexec = QueryExecutionFactory.createServiceRequest(this.serviceURI, queryVar);
		String params = prop.getProperty(Constants.URL_PARAM);
		StringTokenizer paramTokens = new StringTokenizer(params, ";");
		while(paramTokens.hasMoreTokens())
		{
			String token = paramTokens.nextToken();
			qexec.addParam(token, prop.getProperty(token));			
		}
		com.hp.hpl.jena.query.ResultSet rs = qexec.execSelect();
		return rs;
	}

	@Override
	public void execInsertQuery(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.JENA;
	}

	@Override
	public Vector<String> getEntityOfType(String sparqlQuery) {
		// TODO Auto-generated method stub
		// run the query 
		// convert to string
		Vector <String> retString = new Vector<String>();
		ResultSet rs = (ResultSet)execSelectQuery(sparqlQuery);
		
		// gets only the first variable
		Iterator varIterator = rs.getResultVars().iterator();
		String varName = (String)varIterator.next();
		while(rs.hasNext())
		{
			QuerySolution row = rs.next();
			retString.addElement(Utility.getInstanceName(row.get(varName)+""));
		}
		return retString;
	}

	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public void openDB(String propFile) {
		
		// in this case the file would basically tell us what is the name of the file
		// and possibly the format
		try {
			prop = new Properties();
			prop.load(new FileInputStream(propFile));
			this.serviceURI = prop.getProperty(Constants.SPARQL_QUERY_ENDPOINT);
			this.connected = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Boolean execAskQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
