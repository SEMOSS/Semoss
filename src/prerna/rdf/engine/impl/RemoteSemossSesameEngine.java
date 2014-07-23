package prerna.rdf.engine.impl;

import java.io.StringBufferInputStream;
import java.util.Hashtable;
import java.util.Vector;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.Gson;

public class RemoteSemossSesameEngine extends AbstractEngine {
	
	// things this needs to override
	// OpenDB
	// CloseDB
	// Querying of the result
	boolean connected = false;
	String api = null;
	String database = null;
	
	
	
	public void openDB(String propFile)
	{
		// the propfile will typically have 
		// the remote base URI
		// the name of the database
		try {
			
				// get the URI for the remote
				System.err.println("Loading file ENGINE " + propFile);
				
				
				if(propFile != null)
				{
					String baseFolder = DIHelper.getInstance().getProperty(
					"BaseFolder");
					String fileName = baseFolder + "/" + propFile;
					prop = loadProp(propFile);
					
					api = prop.getProperty(Constants.URI);
					database = prop.getProperty("DATABASE");
				}
				engineName = database;
				String insights = Utility.retrieveResult(api + "/s-" + database + "/getInsightDefinition", null);
				
				insightBase = getNewRepository();
				insightVF = insightBase.getValueFactory();
				engineURI2 = insightVF.createURI("database:" + engineName);
				System.out.println("Engine URI is " + engineURI2);
				
				System.out.println("Insights is " + insights);


				// add it to the core connection
				insightBase.add(new StringBufferInputStream(insights), "http://semoss.org", RDFFormat.RDFXML);
				
				// need to do the same with the owl
				String owl = Utility.retrieveResult(api + "/s-" + database + "/getOWLDefinition", null);
				
				RepositoryConnection rc = getNewRepository();

				rc.add(new StringBufferInputStream(owl), "http://semoss.org", RDFFormat.RDFXML);
				baseDataEngine = new RDFFileSesameEngine();
				
				baseDataEngine.rc = rc;
				
				// yup.. we are open for business
				connected = true;
		}catch(Exception ex)
		{
			// will do something later
			ex.printStackTrace();
			connected = false;
		}		
	}
		
	public RepositoryConnection getNewRepository() {
		try {
			RepositoryConnection rc = null;
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();
			return rc;
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Object execGraphQuery(String query)
	{
		// this will call the engine and gets then flushes it into sesame jena construct wrapper
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("query", query);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/execGraphQuery", params);

		Gson gson = new Gson();
		SesameJenaConstructWrapper sjcw = gson.fromJson(output, SesameJenaConstructWrapper.class);
		
		return sjcw;
	}
	

	public Object execSelectQuery(String query)
	{
		// this will call the engine and gets then flushes it into sesame jena construct wrapper
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("query", query);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/execSelectQuery", params);

		Gson gson = new Gson();
		SesameJenaSelectWrapper sjcw = gson.fromJson(output, SesameJenaSelectWrapper.class);
		
		return sjcw;
	}

	public Object execCheaterQuery(String query)
	{
		// this will call the engine and gets then flushes it into sesame jena construct wrapper
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("query", query);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/execCheaterQuery", params);

		Gson gson = new Gson();
		SesameJenaSelectCheater sjcw = gson.fromJson(output, SesameJenaSelectCheater.class);
		
		return sjcw;
	}

	
	// gets the from neighborhood for a given node
	public Vector <String> getFromNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getFromNeighbors", params);

		Gson gson = new Gson();
		Vector <String> fromN = gson.fromJson(output, Vector.class);
		
		return fromN;
	}
	
	// gets the to nodes
	public Vector <String> getToNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getToNeighbors", params);

		Gson gson = new Gson();
		Vector <String> toN = gson.fromJson(output, Vector.class);
		
		return toN;

	}
	
	// gets the from and to nodes
	public Vector <String> getNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getNeighbors", params);

		Gson gson = new Gson();
		Vector <String> nN = gson.fromJson(output, Vector.class);
		
		return nN;
	}

	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE;
	}
	
	
	public void setAPI(String api)
	{
		this.api = api;
	}
	
	public void setDatabase(String database)
	{
		this.database = database;
	}
	
	public Vector<String> getEntityOfType(String sparqlQuery)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("sparqlQuery", sparqlQuery);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getEntityOfType", params);

		Gson gson = new Gson();
		Vector <String> retVector = gson.fromJson(output, Vector.class);
		
		return retVector;

	}
	
	public String getProperty(String key)
	{
		Hashtable params = new Hashtable();
		params.put("key", key);
		String output = Utility.retrieveResult(api + "/s-" + database + "/getProperty", params);
		return output;
	}
	
	// needs
	// get Entity of type
	
	// is connected
	public boolean isConnected()
	{
		return connected;
	}
	
}
