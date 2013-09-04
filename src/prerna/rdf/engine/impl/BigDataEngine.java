package prerna.rdf.engine.impl;

import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

public class BigDataEngine extends AbstractEngine implements IEngine {

	public BigdataSail bdSail = null;
	Properties rdfMap = null;
	public SailRepositoryConnection rc = null;
	public SailConnection sc = null;
	public ValueFactory vf = null;
	boolean connected = false;
	private String engineName;
	InferenceEngine ie = null;
	public boolean queryBoolean = true;
	
	@Override
	public void openDB(String propFile)
	{
		try
		{			
			super.openDB(propFile);
			bdSail = new BigdataSail(prop);
			// BigdataSail.Options.TRUTH_MAINTENANCE = "true";
			BigdataSailRepository repo = new BigdataSailRepository(bdSail);
			repo.initialize();
			rc = repo.getConnection();
		
			// System.out.println("ie forward chaining " + ie);
			// need to convert to constants
			String dbcmFile = prop.getProperty(Constants.DBCM_Prop);
			String workingDir = System.getProperty("user.dir");
			
			sc = ((SailRepositoryConnection) rc).getSailConnection();
			dbcmFile = workingDir + "/" + dbcmFile;
			rdfMap = DIHelper.getInstance().getCoreProp();
			vf = bdSail.getValueFactory();
			this.connected = true;
			
			
	        ie = ((BigdataSail)bdSail).getInferenceEngine();

			
			
			
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
			sc.commit();
			sc.close();
		} catch (SailException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
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
			sagq.setMaxQueryTime(5);
			System.out.println("max query time set");
				res = sagq.evaluate();

		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			queryBoolean = false;
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			queryBoolean = false;
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			queryBoolean = false;
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
		sc.commit();
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
			if(sparqlQuery != null)
				{
					TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
					System.out.println("\nSPARQL: " + sparqlQuery);
					tq.setIncludeInferred(true /* includeInferred */);
					TupleQueryResult sparqlResults = tq.evaluate();
					Vector<String> strVector = new Vector();
					while(sparqlResults.hasNext())
						strVector.add(Utility.getInstanceName(sparqlResults.next().getValue(Constants.ENTITY)+ ""));
		
					return strVector;
				}
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
	public void addStatement(String subject, String predicate, Object object, boolean concept)
	{
		System.out.println("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			Value newObj = null;
			String subString = null;
			String predString = null;
			String objString = null;
			String sub = subject.trim();
			String pred = predicate.trim();
					
			subString = cleanString(sub, false);
			newSub = vf.createURI(subString);
			
			predString = cleanString(pred, false);
			newPred = vf.createURI(predString);
			
			if(!concept)
			{
				if(object.getClass() == new Double(1).getClass())
				{
					System.out.println("Found Double " + object);
					sc.addStatement(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					System.out.println("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.addStatement(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					System.out.println("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.addStatement(newSub, newPred, vf.createLiteral(cleanValue));
				} 
			}
			else
				sc.addStatement(newSub, newPred, vf.createURI(object+""));

		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void infer()
	{
        try {
			ie.computeClosure(null);
			sc.commit();
		} catch (SailException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void commit()
	{
		try {
			sc.commit();
			((BigdataSailRepositoryConnection)rc).flush();
			//closeDB();
			//openDB(propFile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
}
