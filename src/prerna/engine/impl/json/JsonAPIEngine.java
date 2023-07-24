package prerna.engine.impl.json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import cern.colt.Arrays;
import net.minidev.json.JSONArray;
import prerna.engine.impl.AbstractDatabase;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.JsonInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class JsonAPIEngine extends AbstractDatabase {

	private static final Logger logger = LogManager.getLogger(JsonAPIEngine.class);

	private static final String ARRAY = "ARRAY";
	public static final String COUNT = "COUNT";
	private static final String STRING = "String";

	public static final String INPUT_TYPE = "input_type";
	public static final String INPUT_METHOD = "input_method";
	public static final String INPUT_URL = "input_url";
	public static final String INPUT_PARAMS = "input_params";
	public static final String MANDATORY_INPUT = "mandatory_input";
	public static final String OUTPUT_TYPE = "output_type";
	public static final String PATH_PATTERNS = "path_patterns";
	public static final String CONCAT = "concat";
	public static final String DELIM = "delim";
	public static final String REPEATER = "repeater";
	
	Object document = null;

	public void openDB(String propFile) {
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			Hashtable <String, String> paramHash = new Hashtable <>();
			paramHash.put("BaseFolder", baseFolder);
			if(getEngineId() != null)
				paramHash.put("engine", getEngineId());

			if(propFile != null) {
				setPropFile(propFile);
				logger.info("Opening DB - " + Utility.cleanLogString(engineName));
				setPropFile(propFile);
			}
			if(prop != null) {
				// load the rdbms insights db
				// nope nothing to load here

				// load the rdf owl db
				String owlFile = prop.getProperty(Constants.OWL);
				
				if(owlFile != null) {
					File owlF = new File(Utility.normalizePath(owlFile));
					// need a check here to say if I am asking this to be remade or keep what it is
					if(!owlF.exists() || owlFile.equalsIgnoreCase("REMAKE")) {
						// the process of remake will start here
						// see if the usefile is there
						if(this.prop.containsKey(USE_FILE)) {
							String owlFileName = null;
							String dataFile = SmssUtilities.getDataFile(this.prop).getAbsolutePath();
							if(owlFile.equals("REMAKE")) {
								// we will make the name
								File dF = new File(dataFile);
								owlFileName = this.engineName + "_OWL.OWL";
								owlFile = dF.getParentFile() + DIR_SEPARATOR + owlFileName;
							} else {
								owlFileName = FilenameUtils.getName(owlFile);
							}
							
							owlFile = generateOwlFromFlatFile(dataFile, owlFile, owlFileName);
						} else {
							owlFile = null;
						}
					}
					// set the owl file
					if(owlFile != null) {
						owlFile = SmssUtilities.getOwlFile(this.prop).getAbsolutePath();
						logger.info("Loading OWL: " + Utility.cleanLogString(owlFile));
						setOWL(owlFile);
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
			logger.error(Constants.STACKTRACE, e);
		} 
	}
	
	protected void loadDocument()
	{
		try {
			if(prop.containsKey(INPUT_TYPE) && ((String)prop.get(INPUT_TYPE)).equalsIgnoreCase("file"))
				document = Configuration.defaultConfiguration().jsonProvider().parse(new FileInputStream(baseFolder + "/" + Utility.normalizePath(prop.getProperty(INPUT_URL))), "utf-8");
		} catch (FileNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
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
		
		// need some way to identify how to segregate into one vs. many requests
		
		String [] pathParts = query.split("@@@");
		String [] inputParams = null;
		String [] jsonPaths = null;
		Hashtable retHash = new Hashtable();
		
		Object curDoc = getDocument(null);
		//JSONArray [] data = new JSONArray[jsonPaths.length];

		if(pathParts.length == 2)
		{
			jsonPaths = pathParts[0].split(";");
			inputParams = pathParts[1].split(";");		
		}
		else
		{
			jsonPaths = pathParts[0].split(";");			
		}
		// which of the params are list params
		// hopefully there is only one
		// I have no idea how to deal with many at htis point
		// without it being a MESS of for loops
		Hashtable listParams = new Hashtable();
		// since I am hoping there is only one
		// I am keeping it here
		String listKey = null;
		String listValue = null;
		
		boolean array = false;
		for(int paramIndex = 0;inputParams != null && paramIndex < inputParams.length;paramIndex++)
		{
			String [] keyValue = inputParams[paramIndex].split("=");
			String key = keyValue[0];
			String value = keyValue[1];
			
			if(value.startsWith(ARRAY)) {
				array = true;
				listKey = key;
				listValue = value.replace(ARRAY, "");
				listParams.put(key, value.replace(ARRAY, ""));
			}
		}
		
		
		// make the doc
		// get the content
		// if this is not a file based then we need to make the URL
		if(curDoc == null)
		{
			String inputData = "";
			// need to run the http to grab the URL etc. and then load the document
			
			// need to make a check to see if this one vs. many
//			Hashtable inputParamHash = Utility.getParams(prop.getProperty(input_url));
//			Hashtable inputValHash = getParamHash(inputParams);
//			
//			// make the primary hash
//			Hashtable finalValHash = fillParams(inputParamHash, inputValHash);
			Hashtable inputHash = getMandatoryInputs();
			Hashtable finalValHash = getParamHash(inputParams);
			
			finalValHash = fillParams(inputHash, finalValHash);
			
			// replace each value for the key and send it in
			if(array && listValue != null) {
				String [] multiValue = listValue.split("<>");
				for(int valIndex = 0;valIndex < multiValue.length;valIndex++)
				{
					finalValHash.put(listKey, multiValue[valIndex]);
					String url = constructURL(finalValHash);
					
					if(prop.getProperty(INPUT_METHOD).equalsIgnoreCase("GET"))
					{
						inputData = doGet(url);
					}
					else
					{
						inputData = doPost(finalValHash);
					}
				
					curDoc = getDocument(inputData);					
					// send the data to add to it
					retHash = getOutput(curDoc, jsonPaths, retHash, listKey, multiValue[valIndex]);					
				}
			}
			else // this is not a list.. one time pull call it a day
			{
				String url = constructURL(finalValHash);
				if(prop.getProperty(INPUT_METHOD).equalsIgnoreCase("GET"))
					inputData = doGet(url);
				else
					inputData = doPost(finalValHash);
							
				
				curDoc = getDocument(inputData);					
				
				// send the data to add to it
				retHash = getOutput(curDoc, jsonPaths, retHash, null, null);
				
			}
			
		}

		else // it is a file
		{
			retHash = getOutput(curDoc, jsonPaths, retHash, null, null);
		}	
	
		//return the data
		return retHash;
	}
	
	protected Object getDocument(String json)
	{
		Object retNode = document;
		if(json != null) {
			try {
				retNode = Configuration.defaultConfiguration().jsonProvider().parse(json);
			} catch(Exception ex) {
				logger.error(Constants.STACKTRACE, ex);
			}
		}

		return retNode;
	}
	
	protected Hashtable getOutput(Object doc, String [] jsonPaths, Hashtable retHash, String repeaterHeader, String repeaterValue)
	{
	
		JSONArray [] data = null;
		String [] headers = new String[jsonPaths.length];
		
		if(repeaterHeader != null) {
			data = new JSONArray[jsonPaths.length + 1];
			headers = new String[jsonPaths.length + 1];
		}
		else {
			data = new JSONArray[jsonPaths.length];
		}

		int numRows = 0;
		int totalRows = 0;

		JSONArray [] input = null;
		if(retHash.containsKey("DATA"))
			input = (JSONArray [])	retHash.get("DATA");
		
		if(retHash.containsKey(COUNT))
			totalRows = (Integer) retHash.get(COUNT);
		
		boolean foundData = true;
		
		for(int pathIndex = 0;pathIndex < jsonPaths.length;pathIndex++)
		{
			
			String thisHeader = null;
			String thisPath = null;

			if(jsonPaths[pathIndex].contains("="))
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
			
			// need to track for classnot found
			// and PathNotFound exception
			try {
				Object object = JsonPath.read(doc,  thisPath);
	
				if(object instanceof JSONArray)
					data[pathIndex] = (JSONArray)object;
				else // if it is a single item just add it to the list of things
				{
					if(data[pathIndex] == null)
						data[pathIndex] = new JSONArray();
					data[pathIndex].add(object);
				}	
				// add it to the current input
				if(input != null)
					input[pathIndex].addAll(data[pathIndex]);
	
				// set the headers
				headers[pathIndex] = thisHeader;
							
				// if we are starting at a new point
				// add the number of rows to comparator
				
				if(numRows == 0 || numRows > data[pathIndex].size())
					numRows = data[pathIndex].size();
				
				logger.info(" >> " + data[pathIndex].toString());
				logger.info("Length >> " + data[pathIndex].size());	

			} catch(PathNotFoundException ex) {
				logger.info("Path not found.. " + Utility.cleanLogString(thisPath));
				foundData = false;
			}
		}
		
		// add the repeater
		if(repeaterHeader != null && foundData)
		{
			headers[jsonPaths.length] = repeaterHeader;
			
			// fill it with the repeater value
			JSONArray repeaterData = new JSONArray();
			for(int rowIndex = 0;rowIndex < numRows;rowIndex++)
				repeaterData.add(repeaterValue);

			data[jsonPaths.length] = repeaterData;
			
			if(input != null)
				input[jsonPaths.length].addAll(data[jsonPaths.length]);
		}
		
		
		totalRows = totalRows + numRows;
		if(!retHash.containsKey("TYPES"))
			retHash.put("TYPES", getTypes(data));
		retHash.put("HEADERS", headers);
		if(input == null)
			retHash.put("DATA", data);
		else
			retHash.put("DATA", input);
			
		retHash.put(COUNT, totalRows);

		logger.info("Output..  " + Utility.cleanLogString(Arrays.toString(data)));
		
		return retHash;
	}
	
	protected String [] getTypes(Object data2)
	{
		JSONArray [] data = (JSONArray [])data2;
		
		String [] types = new String[data.length];
		for(int dataIndex = 0;data != null && dataIndex < data.length;dataIndex++)
		{
			if(!data[dataIndex].isEmpty()) {
				Object firstOne = data[dataIndex].get(0);
				if(firstOne == null)
					types[dataIndex] = STRING;
				else if(firstOne instanceof Integer)
					types[dataIndex] = "int";
				else if(firstOne instanceof JSONArray)
					types[dataIndex] = STRING;
				else if(firstOne instanceof Double)
					types[dataIndex] = "Double";
				else
					types[dataIndex] = STRING;
			}
		}

		return types;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return prerna.engine.api.IDatabase.ENGINE_TYPE.JSON;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
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

	// get call
	public String doGet(String url)
	{
		String retString = null;
		
		CloseableHttpClient httpclient = null;
		try {
			
			httpclient = HttpClients.createDefault();
			HttpGet httpget = new HttpGet(url);
			
			// need to set headers if the headers are there
			if(prop.containsKey("HEADERS"))
			{
				HashMap headersMap = (HashMap)prop.get("HEADERS");
				
				Iterator keys = headersMap.keySet().iterator();
				while(keys.hasNext())
				{
					String thisKey = (String)keys.next();
					String thisValue = (String)headersMap.get(thisKey);
					// need to fill the headers
					httpget.addHeader(thisKey, thisValue);
				}
			}
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httpget);
			
			retString = handler.handleResponse(response);
		} catch (ClientProtocolException cpe) {
			logger.error(Constants.STACKTRACE, cpe);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
		}
				
		return retString;
	}

	public String doPost(Hashtable params)
	{
		String retString = null;
		CloseableHttpClient httpclient = null;
		try {
			String url = prop.getProperty(INPUT_URL);
			
			httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);

			// need to set headers if the headers are there
			if(prop.containsKey("HEADERS"))
			{
				HashMap headersMap = (HashMap)prop.get("HEADERS");
				
				Iterator keys = headersMap.keySet().iterator();
				while(keys.hasNext())
				{
					String thisKey = (String)keys.next();
					String thisValue = (String)headersMap.get(thisKey);
					// need to fill the headers
					httppost.addHeader(thisKey, thisValue);
				}
			}

			List<NameValuePair> paramList = new ArrayList<>();
			Enumeration <String> keys = params.keys();
			
			while(keys.hasMoreElements())
			{
				String key = keys.nextElement();
				String value = (String)params.get(key);
				
				paramList.add(new BasicNameValuePair(key, value));
			}			
			
			// set entity
			httppost.setEntity(new UrlEncodedFormEntity(paramList));
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			logger.info("Response Code " + response.getStatusLine().getStatusCode());
			
			int status = response.getStatusLine().getStatusCode();
			
			BufferedReader rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			
			retString = result.toString();
		} catch (ClientProtocolException cpe) {
			logger.error(Constants.STACKTRACE, cpe);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
		}
				
		return retString;
	}

	
	// gets this as input stream
	public InputStream doPostI(Hashtable params)
	{
		InputStream retStream = null;
		CloseableHttpClient httpclient = null;
		try {
			String url = prop.getProperty(INPUT_URL);
			
			httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);

			// need to set headers if the headers are there
			if(prop != null && prop.containsKey("HEADERS"))
			{
				HashMap headersMap = (HashMap)prop.get("HEADERS");
				
				Iterator keys = headersMap.keySet().iterator();
				while(keys.hasNext())
				{
					String thisKey = (String)keys.next();
					String thisValue = (String)headersMap.get(thisKey);
					// need to fill the headers
					httppost.addHeader(thisKey, thisValue);
				}
			}

			List<NameValuePair> paramList = new ArrayList<>();
			Enumeration <String> keys = params.keys();
			
			while(keys.hasMoreElements())
			{
				String key = keys.nextElement();
				String value = (String)params.get(key);
				
				paramList.add(new BasicNameValuePair(key, value));
			}			
			
			// set entity
			httppost.setEntity(new UrlEncodedFormEntity(paramList));
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			retStream = response.getEntity().getContent();
		} catch (ClientProtocolException cpe) {
			logger.error(Constants.STACKTRACE, cpe);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
		}
				
		return retStream;
	}


	// get call
	public InputStream doGetI(String url)
	{
		InputStream retStream = null;
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClients.createDefault();
			HttpGet httpget = new HttpGet(url);
			
			// need to set headers if the headers are there
			if(prop!= null && prop.containsKey("HEADERS"))
			{
				HashMap headersMap = (HashMap)prop.get("HEADERS");
				
				Iterator keys = headersMap.keySet().iterator();
				while(keys.hasNext())
				{
					String thisKey = (String)keys.next();
					String thisValue = (String)headersMap.get(thisKey);
					// need to fill the headers
					httpget.addHeader(thisKey, thisValue);
				}
			}
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httpget);

			retStream = response.getEntity().getContent();
		} catch (ClientProtocolException cpe) {
			logger.error(Constants.STACKTRACE, cpe);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
		}
				
		return retStream;
	}

	// get the inputs needed
	private Hashtable getMandatoryInputs()
	{
		Hashtable inputHash = new Hashtable();
		
		String inputs = prop.getProperty(MANDATORY_INPUT);
		
		if(inputs != null && inputs.length() > 0)
		{
			String [] inputArr = inputs.split(";");
			for(int inputIndex = 0;inputIndex < inputArr.length;inputIndex++)
				inputHash.put(inputArr[inputIndex], "EMPTY");
		}
		else
		{
			inputHash = Utility.getParams(prop.getProperty(INPUT_URL));
		}
		
		return inputHash;
	}
	

	// composes the params
	private Hashtable getParamHash(String [] params)
	{
		String [] paramToks = params;
		Hashtable <String, String> paramHash = new Hashtable <>();
	
		if(params != null)
		{
			for(int paramIndex = 0;paramIndex < paramToks.length;paramIndex++)
			{
				String thisParam = paramToks[paramIndex];
				
				String [] thisParamToks = thisParam.split("=");
				
				paramHash.put(thisParamToks[0], thisParamToks[1]);
			}
		}		
		return paramHash;
	}
	
	private Hashtable fillParams(Hashtable inputParam, Hashtable valParam)
	{
		Enumeration <String> keys = inputParam.keys();

		String url = prop.getProperty(INPUT_URL);
		
		Hashtable retHash = valParam;
		retHash.put(INPUT_URL, url);


		// replace with the new values
		while(keys.hasMoreElements())
		{
			String key = keys.nextElement();
			String value = null;
			if(valParam.containsKey(key))
				value = (String)valParam.get(key);
			else if(prop.containsKey(key + "_DEFAULT"))
				value = prop.getProperty(key + "_DEFAULT");
			else
				// need a way to remove this
			{
				// the value to remove is key:@key@ or key=@key@
				url = url.replace(key+ ":@" + key + "@", "");
				url = url.replace(key+ "=@" + key + "@", "");
				url = url.replace("++", "+");
				url = url.replace("&&", "&");
				retHash.put(INPUT_URL, url);
			}
			if(value != null)
				retHash.put(key, value);
		}
		
		return retHash;
	}
	
	// take the URL 
	private String constructURL(Hashtable params)
	{
		// params are given as paramname=value;paramname2=value2
	
		// need to get hate URL
		String inputUrl = (String)params.get(INPUT_URL);
		String mandatoryInputs = prop.getProperty(MANDATORY_INPUT);		
		
		// compose the final URL
		return Utility.fillParam2(inputUrl, params);		
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter(){
		return new JsonInterpreter(this);
	}
}
