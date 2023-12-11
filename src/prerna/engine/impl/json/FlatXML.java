package prerna.engine.impl.json;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;
import prerna.util.Utility;

public class FlatXML {

	// Things I need to get stuff done
	
	// INPUT
	// Input Sequence - if not present, it is just the input URL
	// INPUT_Type - can be a file
	// INPUT_URL - the main URL to hit to get the data from
	// METHOD - Get / Post
	// Parameters - Names of parameters separated by ;
	// Parameter_Default - default value to use for this parameter
	
	// Output
	// OUTPUT_TYPE Table Vs. Graph vs. Hashtable - For now I will only handle table and Hashtable
	// Path_Pattern = All the path patterns separated by ;
	// If not specified the path is treated as a single value - if it it not it is stringified
	// later, we can also add in where the path pattern can have previous zooms. We will come to that in a bit
	// <specific path pattern>= alias header to use

	// the output is finally cobbled into a R datatable through a c()
	private static final Logger logger = LogManager.getLogger(FlatXML.class);
	
	public static final String INPUT_TYPE = "input_type";
	public static final String OUTPUT_TYPE = "output_type";
	public static final String PATH_PATTERNS = "path_patterns";
	public static final String CONCAT = "concat";
	public static final String DELIM = "delim";
	private static final String STACKTRACE = "StackTrace: ";

	
	Properties prop = null;
	Object document = null;
	
//	
//	public static void main(String [] args) throws Exception
//	{
//		// get the XML
//		// convert to json
//		// flatten json
//		FlatXML client = new FlatXML();
//		//String file = "c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\interaction.json";
//		String file = "c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\repos.json";
//		String propFile = "c:\\users\\pkapaleeswaran\\workspacej3\\datasets\\reposProp.prop";
//		//client.getURLData("https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:%22Robitussen%22+patient.reaction.reactionoutcome:%225%22");
//		//client.loadProp(propFile);
//		//client.getData(file);
//		//client.flattenJson(null);
//		client.flattenJsonFromFile(file);
//	}
	
	
	
	
	public void loadProp(String propFile)
	{
		try(FileInputStream fis = new FileInputStream(propFile)) {
			this.prop = new Properties();
			prop.load(fis);
			if(prop.containsKey("input_type") && ((String)prop.get("input_type")).equalsIgnoreCase("file"))
				document = Configuration.defaultConfiguration().jsonProvider().parse(new FileInputStream(Utility.normalizePath(prop.getProperty("input_url"))), "utf-8");

			execQuery(prop.getProperty("path_patterns"));
				
		} catch (FileNotFoundException fnfe) {
			logger.error(STACKTRACE, fnfe);
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		}
	}
	
	public void getURLData(String url)
	{
		String retString = null;
		CloseableHttpClient httpclient  = null;
		try {
			
			httpclient = HttpClients.createDefault();
			HttpGet httpget = new HttpGet(url);
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httpget);
			
			retString = handler.handleResponse(response);
			
			System.out.println(">> " + retString);
		} catch (ClientProtocolException cpe) {
			logger.error(STACKTRACE, cpe);
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} finally {
			if(httpclient != null) {
				try {
					httpclient.close();
				} catch(IOException e) {
					// ignore
				}
			}
		}
				
		//return retString;

	}
	
	public Object execQuery(String pathPatterns)
	{
		// the exec query has to get a couple of different things
		// parameters for the input
		// selection for the output
		// output metadata - may not be needed
		// need to use a better delimiter
		String [] pathParts = pathPatterns.split("@@@");
		String [] inputParams = null;
		String [] jsonPaths = null;
		String [] types = null; 
		if(pathParts.length == 2)
		{
			inputParams = pathParts[0].split(";");		
			jsonPaths = pathParts[1].split(";");
			
			// process input here
		}
		else
		{
			jsonPaths = pathParts[0].split(";");			
		}
				
		JSONArray [] data = new JSONArray[jsonPaths.length];
		types = new String[jsonPaths.length];
		int numRows = 0;
		StringBuffer headers = null;
		
		for(int pathIndex = 0;pathIndex < jsonPaths.length;pathIndex++)
		{
			data[pathIndex] = JsonPath.read(document, jsonPaths[pathIndex]);
			
			if(headers == null)
				headers = new StringBuffer(prop.getProperty(jsonPaths[pathIndex]));
			else
				headers.append(",").append(prop.getProperty(jsonPaths[pathIndex]));
						
			if(numRows == 0 || numRows > data[pathIndex].size())
				numRows = data[pathIndex].size();
			
			logger.debug(" >> " + data[pathIndex].toString());
			logger.debug("Length >> " + data[pathIndex].size());
			
			Object firstOne = data[pathIndex].get(0);
			if(firstOne instanceof Integer)
				types[pathIndex] = "int";
			if(firstOne instanceof JSONArray)
				types[pathIndex] = "String";
			if(firstOne instanceof Double)
				types[pathIndex] = "Double";
		}
		
		logger.debug("Output..  " + data);
		
		Hashtable retHash = new Hashtable();
		retHash.put("TYPES", types);
		retHash.put("HEADERS", headers);
		retHash.put("DATA", data);
		retHash.put("COUNT", numRows);

		if (headers != null) {
			writeCSV(data, "random.csv", numRows, headers.toString());
		}

		return retHash;
	}
	
	public void writeCSV(JSONArray [] data, String fileName, int numRows, String headers)
	{
		// I need to see the number of elements and based on that process through the stuff
		String [] cols = new String [data.length];
		int numItems = data.length;
		
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			bw.write(headers);
			bw.write("\n");
			
			for(int rowIndex = 0;rowIndex < numRows;rowIndex++)
			{
				// for each row get hte column and print
			
				StringBuffer composer = null;
				
				for(int colIndex = 0;colIndex < cols.length;colIndex++)
				{
					JSONArray thisArray = data[colIndex];
					
					String thisData = thisArray.get(rowIndex).toString().replace(",","_");
					
					if(composer == null)
						composer = new StringBuffer(thisData);
					else
						composer = composer.append(",").append(thisData);
				}

				if (composer != null) {
					bw.write(composer.toString());
				}
				bw.write("\n");
			}
			
		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		} finally {
			if(bw != null) {
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void getData(String file)
	{
		InputStream inputStream = null;
		Reader reader  = null;
		BufferedReader breader = null;
		try {
			inputStream = new FileInputStream(file);
			reader = new InputStreamReader(inputStream);
			breader = new BufferedReader(reader);
			
			String jsonString = breader.readLine();
			logger.debug("String is .. " + jsonString);

			Object parsedDocument = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);

			// getting a particular element
			//Object rxcuis = JsonPath.read(parsedDocument, "$..[?(@.rxcui == \"75207\")]");
			
			// getting from an array
			//Object rxcuis = JsonPath.read(parsedDocument, "$..[?(\"656659\" in @.['rxcuis'])]");
			
			Object rxcuis = JsonPath.read(parsedDocument, "$..minConcept[*].rxcui");
			
			
			//Object severity = JsonPath.read(parsedDocument, "$..name");

			// get all interaction pairs where there is a severity of high
			JSONArray severity = JsonPath.read(parsedDocument, "$..interactionPair[?(@.severity == \"high\")])");
			
			String severityStr = severity.toJSONString();
			Object document2 = Configuration.defaultConfiguration().jsonProvider().parse(severityStr);

			// get the name from min concept
			Object name = JsonPath.read(document2, "$..minConceptItem['name']");
			

			logger.debug("Value..  " + rxcuis + " \n" + severity);

			logger.debug("Value..  " + rxcuis + " \n" + name);

		} catch (InvalidJsonException ije) {
			logger.error(STACKTRACE, ije);
		} catch (FileNotFoundException fnfe) {
			logger.error(STACKTRACE, fnfe);
		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} finally {
			if(inputStream != null) {
		          try {
		        	  inputStream.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
			if(reader != null) {
		          try {
		        	  reader.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
			if(breader != null) {
		          try {
		        	  breader.close();
		          } catch(IOException e) {
		            // ignore
		          }
		        }
		}
	}
	
	public Map <String, Object> flattenJson(String json)
	{
		if(json == null)
			json = "{ \"a\" : { \"b\" : 1, \"c\": null, \"d\": [false, true] }, \"e\": \"f\", \"g\":2.3 }";
		Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(json);
		
		String output = JsonFlattener.flatten(json);
		
		logger.debug(flattenJson);
		logger.debug(output);
		
		return flattenJson;
	}

	public Map <String, Object> flattenJsonFromFile(String file) throws Exception
	{
		InputStream inputStream = new FileInputStream(file);
		Reader reader = new InputStreamReader(inputStream);
		//BufferedReader breader = new BufferedReader(reader);
		
		//String jsonString = breader.readLine();
		//System.out.println(jsonString);

		//flattenJson(jsonString);
		// Support Reader as input 
		JsonFlattener jf = new JsonFlattener(reader);
		
		Map<String, Object> flattenJson = jf.flattenAsMap();
		
		//System.out.println(">>> " + jf.flatten());
		
		//System.out.println(flattenJson);
		
		//System.out.println(output);
		
		return flattenJson;
	}

	
}
