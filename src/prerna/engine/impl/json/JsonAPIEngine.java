package prerna.engine.impl.json;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import net.minidev.json.JSONArray;

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.impl.AbstractEngine;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.query.interpreters.JsonInterpreter;
import prerna.query.interpreters.SparqlInterpreter2;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

public class JsonAPIEngine extends AbstractEngine {
	
	private static final Logger logger = LogManager.getLogger(AbstractEngine.class.getName());
	
	public static final String input_type = "input_type";
	public static final String input_method = "input_method";
	public static final String input_url = "input_url";
	public static final String input_params = "input_params";
	public static final String mandatory_input = "mandatory_input";
	public static final String output_type = "output_type";
	public static final String path_patterns = "path_patterns";
	public static final String concat = "concat";
	public static final String delim = "delim";

	
	Object document = null;

	
	public void openDB(String propFile) {
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			Hashtable <String, String> paramHash = new Hashtable <String, String>();
			paramHash.put("BaseFolder", baseFolder);
			if(getEngineName() != null)
				paramHash.put("engine", getEngineName());

			if(propFile != null) {
				setPropFile(propFile);
				logger.info("Opening DB - " + engineName);
				prop = Utility.loadProperties(propFile);
			}
			if(prop != null) {
				// load the rdbms insights db
				// nope nothing to load here

				// load the rdf owl db
				String owlFile = prop.getProperty(Constants.OWL);
				
				if (owlFile != null) {
					// need a check here to say if I am asking this to be remade or keep what it is
					if(owlFile.equalsIgnoreCase("REMAKE"))
					{
						// the process of remake will start here
						// see if the usefile is there
						if(prop.containsKey(USE_FILE))
						{
							String csvFile = prop.getProperty(DATA_FILE);	
							owlFile = csvFile.replace("data/", "") + ".OWL";
							//Map <String, String> paramHash = new Hashtable<String, String>();
							
							paramHash.put("BaseFolder", DIHelper.getInstance().getProperty("BaseFolder"));
							paramHash.put("ENGINE", getEngineName());
							csvFile = Utility.fillParam2(csvFile, paramHash);

							
							String fileName = Utility.getOriginalFileName(csvFile);
							// make the table name based on the fileName
							String cleanTableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
							owlFile = baseFolder + "/db/" + getEngineName() + "/" + cleanTableName + ".OWL";
							
							CSVToOwlMaker maker = new CSVToOwlMaker();
							maker.makeOwl(csvFile, owlFile, getEngineType());
							owlFile = "/db/" + getEngineName() + "/" + cleanTableName + ".OWL";
							
							if(prop.containsKey("REPLACE_OWL"))
								Utility.updateSMSSFile(propFile, Constants.OWL, owlFile);
						}
						else
							owlFile = null;
					}
					if(owlFile != null)
					{					
						owlFile = Utility.fillParam2(owlFile, paramHash);
						logger.info("Loading OWL: " + owlFile);
						setOWL(baseFolder + "/" + owlFile);
					}
				}
				// load properties object for db
				// not sure what this is doing
				// I changed this to public
				String genEngPropFile = prop.getProperty(Constants.ENGINE_PROPERTIES);
				if (genEngPropFile != null) {
					generalEngineProp = Utility.loadProperties(baseFolder + "/" + genEngPropFile);
				}
			}
			loadDocument();
			// seems like I may not need this
			// setOWL already does this
			//this.owlHelper = new MetaHelper(baseDataEngine, getEngineType(), this.engineName);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} 
	}

	private void loadDocument()
	{
		try {
			if(prop.containsKey("input_type") && ((String)prop.get("input_type")).equalsIgnoreCase("file"))
				document = Configuration.defaultConfiguration().jsonProvider().parse(new FileInputStream(baseFolder + "/" + prop.getProperty("input_url")), "utf-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// inputparams@@@alias patterns=alias or just alias@@@metadata - I will not worry about metadata for now
	@Override
	public Object execQuery(String query) {
		// the exec query has to get a couple of different things
		// parameters for the input
		// selection for the output
		// output metadata - may not be needed
		// need to use a better delimiter
		String [] pathParts = query.split("@@@");
		String [] inputParams = null;
		String [] jsonPaths = null;
		String [] types = null; 
		
		Object curDoc = document;
		
		
		if(pathParts.length == 2)
		{
			inputParams = pathParts[0].split(";");		
			jsonPaths = pathParts[1].split(";");			
		}
		else
		{
			jsonPaths = pathParts[0].split(";");			
		}

		// make the doc
		if(curDoc == null)
		{
			String inputData = "";
			// need to run the http to grab the URL etc. and then load the document
			if(prop.getProperty(input_method).equalsIgnoreCase("GET"))
				inputData = doGet(inputParams);
			curDoc = Configuration.defaultConfiguration().jsonProvider().parse(inputData);
		}

		JSONArray [] data = new JSONArray[jsonPaths.length];
		types = new String[jsonPaths.length];
		int numRows = 0;
		String [] headers = new String[jsonPaths.length];
		
		for(int pathIndex = 0;pathIndex < jsonPaths.length;pathIndex++)
		{
			String thisHeader = null;
			String thisPath = null;

			if(jsonPaths[pathIndex].equalsIgnoreCase("="))
			{
				String [] pathToks = jsonPaths[pathIndex].split("=");
				thisHeader = pathToks[0];
				thisPath = pathToks[1];
			}
			else
			{
				thisPath = prop.getProperty(jsonPaths[pathIndex]);
				thisHeader = jsonPaths[pathIndex];
			}
			
			data[pathIndex] = JsonPath.read(document, thisPath);

			// set the headers
			headers[pathIndex] = thisHeader;
						
			if(numRows == 0 || numRows > data[pathIndex].size())
				numRows = data[pathIndex].size();
			
			System.out.println(" >> " + data[pathIndex].toString());
			System.out.println("Length >> " + data[pathIndex].size());
			
			Object firstOne = data[pathIndex].get(0);
			if(firstOne instanceof Integer)
				types[pathIndex] = "int";
			if(firstOne instanceof JSONArray)
				types[pathIndex] = "String";
			if(firstOne instanceof Double)
				types[pathIndex] = "Double";
		}
		
		System.out.println("Output..  " + data);
		
		Hashtable retHash = new Hashtable();
		retHash.put("TYPES", types);
		retHash.put("HEADERS", headers);
		retHash.put("DATA", data);
		retHash.put("COUNT", numRows);
		
		//return the data
		return retHash;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return prerna.engine.api.IEngine.ENGINE_TYPE.JSON;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub

	}

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// get call
	private String doGet(String [] params)
	{
		String retString = null;
		
		try {
			String url = constructURL(params);
			
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpGet httpget = new HttpGet(url);
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httpget);
			
			retString = handler.handleResponse(response);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return retString;
	}

	private String doPost(String [] params)
	{
		String retString = null;
		
		try {
			String url = prop.getProperty(input_url);
			
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);
			
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			Hashtable <String, String> paramHash = getParamHash(params);
			Enumeration <String> keys = paramHash.keys();
			
			while(keys.hasMoreElements())
			{
				String key = keys.nextElement();
				String value = paramHash.get(key);
				
				paramList.add(new BasicNameValuePair(key, value));
			}			
			
			// set entity
			httppost.setEntity(new UrlEncodedFormEntity(paramList));
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			retString = handler.handleResponse(response);
			
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return retString;
	}


	// composes the params
	private Hashtable getParamHash(String [] params)
	{
		String [] paramToks = params;
		Hashtable <String, String> paramHash = new Hashtable <String, String>();
		
		for(int paramIndex = 0;paramIndex < paramToks.length;paramIndex++)
		{
			String thisParam = paramToks[paramIndex];
			
			String [] thisParamToks = thisParam.split("=");
			
			paramHash.put(thisParamToks[0], thisParamToks[1]);
		}
		
		return paramHash;
	}
	
	// take the URL 
	private String constructURL(String [] params)
	{
		// params are given as paramname=value;paramname2=value2
	
		// need to get hate URL
		String inputUrl = prop.getProperty(input_url);
		String mandatoryInputs = prop.getProperty(mandatory_input);
		
		// get hte params
		String [] paramToks = params;
		Hashtable <String, String> paramHash = getParamHash(params);
		
		Hashtable inputParamHash = Utility.getParamTypeHash(inputUrl);
		
		Enumeration <String> keys = inputParamHash.keys();
		
		// replace with the new values
		while(keys.hasMoreElements())
		{
			String key = keys.nextElement();
			String value = paramHash.get(key);
			paramHash.put(key, value);
		}
		
		// compose the URL
		String finalURL = Utility.fillParam2(inputUrl, paramHash);
		
		return finalURL;
		
	}
	
	@Override
	public IQueryInterpreter2 getQueryInterpreter2(){
		return new JsonInterpreter(this);
	}

}
