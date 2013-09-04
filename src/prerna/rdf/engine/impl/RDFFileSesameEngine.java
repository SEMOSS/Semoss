package prerna.rdf.engine.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
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
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.sail.BigdataSail;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class RDFFileSesameEngine extends AbstractEngine implements IEngine {

	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	String rdfFileType = "RDF/XML";
	public String baseURI = null;
	public String fileName = null;
	SailConnection sc = null;
	boolean connected = false;
	
	
	@Override
	public void openDB(String propFile2)
	{
		try
		{
			super.openDB(propFile2);
			Repository myRepository = new SailRepository(
	                new ForwardChainingRDFSInferencer(
	                new MemoryStore()));
			myRepository.initialize();
			
			System.err.println("Prop File is " + propFile2);
			
			if(prop != null)
			{
				
				fileName = System.getProperty("user.dir") + "/" + prop.getProperty(Constants.RDF_FILE_NAME);
				rdfFileType = prop.getProperty(Constants.RDF_FILE_TYPE);
				baseURI = prop.getProperty(Constants.RDF_FILE_BASE_URI);
			}

			File file = new File( fileName);
			rc = myRepository.getConnection();
			sc = ((SailRepositoryConnection) rc).getSailConnection();
			vf = rc.getValueFactory();
			
			if(file != null)
			{
				if(rdfFileType.equalsIgnoreCase("RDF/XML")) rc.add(file, baseURI, RDFFormat.RDFXML);
				else if(rdfFileType.equalsIgnoreCase("TURTLE")) rc.add(file, baseURI, RDFFormat.TURTLE);
				else if(rdfFileType.equalsIgnoreCase("BINARY")) rc.add(file, baseURI, RDFFormat.BINARY);
				else if(rdfFileType.equalsIgnoreCase("N3")) rc.add(file, baseURI, RDFFormat.N3);
				else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) rc.add(file, baseURI, RDFFormat.NTRIPLES);
				else if(rdfFileType.equalsIgnoreCase("TRIG")) rc.add(file, baseURI, RDFFormat.TRIG);
				else if(rdfFileType.equalsIgnoreCase("TRIX")) rc.add(file, baseURI, RDFFormat.TRIX);
			}
		    this.connected = true;
		}catch(Exception ignored)
		{
			this.connected = false;
			ignored.printStackTrace();
		}
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
		sc.commit();
	}

	
	public void exportDB() throws Exception
	{
		System.err.println("Exporting database");
		rc.export(new RDFXMLPrettyWriter(new FileWriter(fileName)));
	}

	
}
