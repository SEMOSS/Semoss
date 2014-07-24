/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.util;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.cert.X509Certificate;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.commons.httpclient.HttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

import com.ibm.icu.text.DecimalFormat;

/**
 * The Utility class contains a variety of miscellaneous functions implemented extensively throughout SEMOSS.
 * Some of these functionalities include getting concept names, printing messages, loading engines, and writing Excel workbooks.
 */
public class Utility {

	public static int id = 0;

	/**
	 * Matches the given query against a specified pattern.
	 * While the next substring of the query matches a part of the pattern, set substring as the key with EMPTY constants (@@) as the value
	 * @param 	Query.

	 * @return 	Hashtable of queries to be replaced */
	public static Hashtable getParams(String query)	{	
		Hashtable paramHash = new Hashtable();		
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[\\w/.:]+[@]");

		Matcher matcher = pattern.matcher(query);
		while(matcher.find()) {
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			System.out.println(data);
			// put something to strip the @
			paramHash.put(data, Constants.EMPTY);
		}

		return paramHash;
	}

	/**
	 * Matches the given query against a specified pattern.
	 * While the next substring of the query matches a part of the pattern, set substring as the key with EMPTY constants (@@) as the value
	 * @param 	Query.

	 * @return 	Hashtable of queries to be replaced */
	public static Hashtable getParamTypeHash(String query)	{	
		Hashtable paramHash = new Hashtable();		
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[\\w/.:]+[@]");

		Matcher matcher = pattern.matcher(query);
		while(matcher.find()) {
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			String paramName = data.substring(0, data.indexOf("-"));
			String paramValue = data.substring(data.indexOf("-") + 1);
			
			System.out.println(data);
			// put something to strip the @
			paramHash.put(paramName, paramValue);
		}

		return paramHash;
	}

	/**
	 * Matches the given query against a specified pattern.
	 * While the next substring of the query matches a part of the pattern, set substring as the key with EMPTY constants (@@) as the value
	 * @param 	Query.

	 * @return 	Hashtable of queries to be replaced */
	public static String normalizeParam(String query)	{	
		Hashtable paramHash = new Hashtable();		
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[\\w/.:]+[@]");

		Matcher matcher = pattern.matcher(query);
		while(matcher.find()) {
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			String paramName = data.substring(0, data.indexOf("-"));
			String paramValue = data.substring(data.indexOf("-") + 1);
			
			System.out.println(data);
			// put something to strip the @
			paramHash.put(data, "@"+ paramName + "@");
		}
		 
		return fillParam(query, paramHash);
	}

	
	/**
	 * Gets the param hash and replaces certain queries
	 * @param 	Original query
	 * @param 	Hashtable of format [String to be replaced] [Replacement]

	 * @return 	If applicable, returns the replaced query */
	public static String fillParam(String query, Hashtable paramHash) {
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		System.out.println("Param Hash is " + paramHash);

		Enumeration keys = paramHash.keys();
		while(keys.hasMoreElements()) {
			
			String key = (String)keys.nextElement();
			String value = (String)paramHash.get(key);
			System.out.println("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if(!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}

		return query;
	}

	/**
	 * Splits up a URI into tokens based on "/" character and uses logic to return the instance name.
	 * @param String		URI to be split into tokens.

	 * @return String		Instance name. */
	public static String getInstanceName(String uri) {
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String instanceName = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok) {
				tokens.nextToken();
			} else if (tokIndex + 1 == totalTok) {
				instanceName = tokens.nextToken();
			} else {
				tokens.nextToken();
			}
		}

		return instanceName;
	}

	/**
	 * Go through all URIs in list, splits them into tokens based on "/", and uses logic to return the instance names.
	 * @param Vector<String>	List of URIs to be tokenized.

	 * @return 					Hashtable with instance name as the keys mapped to the tokens as values */
	public static Hashtable<String, String> getInstanceNameViaQuery(Vector<String> uri)
	{

		if (uri.isEmpty()) {
			return new Hashtable<String, String>();
		}
		
		Hashtable<String, String> retHash = new Hashtable<String, String>();
		
		//		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		//
		//		// get the selected repository
		//		Object [] repos = (Object [])list.getSelectedValues();
		//		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
		//		// loads all of the labels
		//		// http://www.w3.org/2000/01/rdf-schema#label
		//		String labelQuery = "";
		//		
		//		//fill all uri for binding string
		//		StringBuffer bindingStr = new StringBuffer("");
		//		for (int i = 0; i<uri.size();i++)
		//		{
		//			if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		//				bindingStr = bindingStr.append("(<").append(uri.get(i)).append(">)");
		//			else
		//				bindingStr = bindingStr.append("<").append(uri.get(i)).append(">");
		//		}
		//		Hashtable paramHash = new Hashtable();
		//		paramHash.put("FILTER_VALUES",  bindingStr.toString());
		//		if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		//		{			
		//			labelQuery = "SELECT DISTINCT ?Entity ?Label WHERE " +
		//					"{{?Entity <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
		//					"}" +"BINDINGS ?Entity {@FILTER_VALUES@}";
		//		}
		//		else if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		//		{
		//			labelQuery = "SELECT DISTINCT ?Entity ?Label WHERE " +
		//					"{{VALUES ?Entity {@FILTER_VALUES@}"+
		//					"{?Entity <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
		//					"}";
		//		}
		//		labelQuery = Utility.fillParam(labelQuery, paramHash);
		//		
		//		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//		sjsw.setEngine(engine);
		//		sjsw.setQuery(labelQuery);
		//		System.out.println(labelQuery);
		//		sjsw.executeQuery();
		//		sjsw.getVariables();
		//		while(sjsw.hasNext())
		//		{
		//			SesameJenaSelectStatement st = sjsw.next();
		//			String label = st.getVar("Label")+"";
		//			label = label.substring(1,label.length()-1);
		//			String uriValue = st.getRawVar("Entity")+ "";
		//			uri.removeElement(uriValue);
		//			retHash.put(label,  uriValue);
		//		}
		//		
		
		for (int i=0;i<uri.size();i++) {
			String uriValue = uri.get(i);
			StringTokenizer tokens = new StringTokenizer(uriValue + "", "/");
			int totalTok = tokens.countTokens();
			String instanceName = null;

			for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
				if (tokIndex + 2 == totalTok) {
					tokens.nextToken();
				} else if (tokIndex + 1 == totalTok) {
					instanceName = tokens.nextToken();
				} else {
					tokens.nextToken();
				}

			}
			retHash.put(instanceName, uriValue);
		}
		return retHash;
	}

	/**
	 * Executes a query on a specific engine, iterates through variables from the sesame wrapper, and uses logic to obtain the concept URI.
	 * @param 	Specified engine.
	 * @param 	Subject URI.

	 * @return 	Concept URI. */
	public static String getConceptType(IEngine engine, String subjectURI) {
		if(!subjectURI.startsWith("http://")) {
			return "";
		}
		
		String query = DIHelper.getInstance().getProperty(Constants.SUBJECT_TYPE_QUERY);
		Hashtable<String, String> paramHash = new Hashtable<String, String>();
		paramHash.put("ENTITY", subjectURI);
		query = Utility.fillParam(query,  paramHash);
		SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		sjw.setEngine(engine);
		sjw.setEngineType(engine.getEngineType());
		sjw.setQuery(query);
		sjw.executeQuery();
		String [] vars = sjw.getVariables();
		String returnType = null;
		while(sjw.hasNext()) {
			SesameJenaSelectStatement stmt = sjw.next();
			String objURI = stmt.getRawVar(vars[0])+"";
			if (!objURI.equals(DIHelper.getInstance().getProperty(Constants.SEMOSS_URI)+"/Concept")) {
				returnType = objURI;
			}

		}
		if (returnType ==null) {
			returnType = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI)+"/Concept";
		}
		
		return returnType;
	}

	/**
	 * Splits up a URI into tokens based on "/" delimiter and uses logic to return the class name.
	 * @param 	URI.

	 * @return 	Name of class. */
	public static String getClassName(String uri) {
		// there are three patterns
		// one is the /
		// the other is the #
		// need to have a check upfront to see 

		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String className = null;
		
		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok) {
				className = tokens.nextToken();
			} else if (tokIndex + 1 == totalTok) {
				tokens.nextToken();
			} else {
				tokens.nextToken();
			}

		}
		
		return className;
	}

	/**
	 * Increases the counter and gets the next ID for a URI.

	 * @return Next ID */
	public static String getNextID() {
		id++;
		return Constants.BLANK_URL + "/" + id;
	}

	/**
	 * Gets the instance and class names for a specified URI and creates the qualified class name.
	 * @param 	URI.

	 * @return 	Qualified URI. */
	public static String getQualifiedClassName(String uri) {
		// there are three patterns
		// one is the /
		// the other is the #
		// need to have a check upfront to see 

		String instanceName = getInstanceName(uri);

		String className = getClassName(uri);
		String qualUri = "";
		if(uri.indexOf("/") >= 0) {
			instanceName = "/" + instanceName;
		}
		
		// remove this in the end
		if(className==null) {
			qualUri = uri.replace(instanceName, "");
		} else {
			qualUri = uri.replace(className+instanceName, className);
		}

		return qualUri;
	}

	/**
	 * Checks to see if a string contains a particular pattern. Used when adding relations.
	 * @param 	Pattern
	 * @param 	String to compare to the pattern

	 * @return 	True if the next token is greater than or equal to zero. */
	public static boolean checkPatternInString(String pattern, String string) {
		// ok.. before you think that this is so stupid why wont you use the regular java.lang methods.. consider the fact that this could be a ; delimited pattern
		boolean matched = false;
		StringTokenizer tokens = new StringTokenizer(pattern, ";");
		while(tokens.hasMoreTokens() && !matched) {
			matched = string.indexOf(tokens.nextToken()) >= 0;
		}
		
		return matched;	
	}

	/**
	 * Used when selecting a repository.
	 * @param 	Used to retrieve successive elements.
	 * @param 	Size.

	 * @return 	List of returned strings */
	public static Vector<String> convertEnumToStringVector(Enumeration enums, int size) {
		Vector<String> retString = new Vector<String>();
		while(enums.hasMoreElements()) {
			retString.add((String) enums.nextElement());
		}
		return retString;
	}

	/**
	 * Runs a check to see if calculations have already been performed.
	 * @param Query (calculation) to be run on a specific engine.

	 * @return 	True if calculations have been performed. */
	public static boolean runCheck(String query) {
		boolean check = true;

		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		List<String> repos = list.getSelectedValuesList();

		SesameJenaSelectWrapper selectWrapper = null;
		for(String s : repos) {
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(s);
			
			selectWrapper = new SesameJenaSelectWrapper();
			selectWrapper.setEngine(engine);
			selectWrapper.setQuery(query);
			selectWrapper.executeQuery();
		}
		
		//if the wrapper is not empty, calculations have already been performed.
		if(!selectWrapper.hasNext()) {
			check = false;
		}
		
		return check;
	}

	/**
	 * Displays error message.
	 * @param Text to be displayed.
	 */
	public static void showError(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text, "Error", JOptionPane.ERROR_MESSAGE);
	}
	
	/**
	 * Displays confirmation message.
	 * @param Text to be displayed.
	 */
	public static void showConfirm(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showConfirmDialog(playPane, text);

	}
	
	/**
	 * Displays a message on the screen.
	 * @param Text to be displayed.
	 */
	public static void showMessage(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text);

	}
	
	/**
	 * Method round.
	 * @param valueToRound double
	 * @param numberOfDecimalPlaces int

	 * @return double */
	public static double round(double valueToRound, int numberOfDecimalPlaces) {
		double multipicationFactor = Math.pow(10, numberOfDecimalPlaces);
		double interestedInZeroDPs = valueToRound * multipicationFactor;
		return Math.round(interestedInZeroDPs) / multipicationFactor;
	}

	/**
	 * Used to round a value to a specific number of decimal places.
	 * @param 	Value to round.
	 * @param 	Number of decimal places to round to.

	 * @return 	Rounded value. */
	public static String sciToDollar(double valueToRound) {
		double roundedValue = Math.round(valueToRound);
		DecimalFormat df = new DecimalFormat("#0");
		NumberFormat formatter = NumberFormat.getCurrencyInstance();
		df.format(roundedValue);
		String retString = formatter.format(roundedValue);
		return retString;
	}

	/**
	 * Loads an engine - sets the core properties, loads base data engine and ontology file.
	 * @param 	Filename.
	 * @param 	List of properties.

	 * @return 	Loaded engine. */
	public static IEngine loadEngine(String fileName, Properties prop) {
		IEngine engine = null;

		System.out.println("In Utility file name is " + fileName);
		try {
			String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

			String engineName = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
			engine = (IEngine)Class.forName(engineClass).newInstance();
			engine.setEngineName(engineName);
			if(prop.getProperty("MAP") != null) {
				engine.setMap(prop.getProperty("MAP"));
			}
			engine.openDB(fileName);
			engine.setDreamer(prop.getProperty(Constants.DREAMER));
			engine.setOntology(prop.getProperty(Constants.ONTOLOGY));
			
			// set the core prop
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
			if(prop.containsKey(Constants.OWL)) {
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
				engine.setOWL(prop.getProperty(Constants.OWL));
			}
			
			//load base data engine and basefilterhash--used for graph play sheet and entity filler
			/*if(engine instanceof AbstractEngine) {
				//need to load the ontology file to get base data
				Properties engineCoreProp = new Properties();
				try {
					engineCoreProp.load(new FileInputStream(prop.getProperty(Constants.ONTOLOGY)));
				} catch (FileNotFoundException e) {
					//showError("File not found: " + prop.getProperty(Constants.ONTOLOGY));
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				//createBaseRelationEngine(engineCoreProp, (AbstractEngine)engine);
			}*/
			
			// set the engine finally
			engines = engines + ";" + engineName;
			DIHelper.getInstance().setLocalProperty(engineName, engine);
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return engine;
	}

	// TODO: TO BE REMOVED
	/**
	 * Creates base relations for a specific engine and RDF Map.
	 * Splits RDF map values into tokens in order to obtain the subject/predicate/object triple.
	 * Put these values into the base filter hash and then add the triple into the base relation engine.
	 * @param 	RDF map
	 * @param 	The base sesame engine for the RDF map

	 * @return 	Hashtable containing base triple relations */
	private static Hashtable createBaseRelations(Properties rdfMap, RDFFileSesameEngine baseRelEngine) throws Exception {
		String relationName = "BaseData";
		Hashtable baseFilterHash = new Hashtable();
		if(rdfMap.containsKey(relationName)){ //load using what is on the map
			String value = rdfMap.getProperty(relationName);
			System.out.println(" Relations are " + value);
			StringTokenizer relTokens = new StringTokenizer(value, ";");
			while (relTokens.hasMoreTokens()) {
				String rel = relTokens.nextToken();
				String relNames = rdfMap.getProperty(rel);
				StringTokenizer rdfTokens = new StringTokenizer(relNames, ";");
				
				while (rdfTokens.hasMoreTokens()) {
					StringTokenizer stmtTokens = new StringTokenizer(rdfTokens.nextToken(), "+");
					String subject = stmtTokens.nextToken();
					String predicate = stmtTokens.nextToken();
					String object = stmtTokens.nextToken();
					baseFilterHash.put(subject, subject);
					baseFilterHash.put(object, object);
					baseFilterHash.put(predicate, predicate);
					// create the statement now
					baseRelEngine.addStatement(subject, predicate, object, true);
				}// statement while
			}// relationship while
		}//if using map
		return baseFilterHash;
	}

	/**
	 * Checks for an OWL and adds it to the engine. 
	 * Sets the base data hash from the engine properties, commits the database, and creates the base relation engine.
	 * @param 	List of properties for a specific engine
	 * @param 	Engine to set
	 */
	private static void createBaseRelationEngine(Properties engineProp, AbstractEngine engine)
	{
		RDFFileSesameEngine baseRelEngine = new RDFFileSesameEngine();
		// If OWL file doesn't exist, go the old way and create the base relation engine
		String owlFileName = (String)DIHelper.getInstance().getCoreProp().get(engine.getEngineName() + "_" + Constants.OWL);
		if(owlFileName == null)
		{
			owlFileName = "./db/" + engine.getEngineName() + "/" + engine.getEngineName() + ".OWL";
		}
		baseRelEngine.fileName = owlFileName;
		baseRelEngine.openDB(null);
		engine.addConfiguration(Constants.OWL, owlFileName);
		if(engineProp.containsKey("BaseData")) {
			//TODO: Need to find a way to write this into the prop file
			try {
				Hashtable hash = createBaseRelations(engineProp, baseRelEngine);
				engine.setBaseHash(hash);
			} catch (Exception e) {
				// TODO: Specify exception
				e.printStackTrace();
			}
		}			
		baseRelEngine.commit();
		engine.setBaseData(baseRelEngine);
	}
	// TODO: TO BE REMOVED

	/**
	 * Cleans a string based on certain patterns
	 * @param 	Original string
	 * @param 	If true, replace forward slashes ("/") with dashes ("-")

	 * @return 	Cleaned string */
	public static String cleanString(String original, boolean replaceForwardSlash){
		String retString = original;
		
		retString = retString.trim();
		retString = retString.replaceAll("\t", " ");//replace tabs with spaces
		while (retString.contains("  ")){
			retString = retString.replace("  ", " ");
		}
		retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		retString = retString.replaceAll("\\{", "(");
		retString = retString.replaceAll("\\}", ")");
		retString = retString.replaceAll("\\\\", "-");//replace backslashes with dashes
		retString = retString.replaceAll("'", "");//remove apostrophe
		retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
		if(replaceForwardSlash) {
			retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
		}
		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		retString = retString.replaceAll("\n", " ");
		retString = retString.replaceAll("<", "(");
		retString = retString.replaceAll(">", ")");

		return retString;
	}

	/**
	 * Creates an excel workbook
	 * @param wb 		XSSFWorkbook to write to
	 * @param fileLoc 	String containing the path to save the workbook
	 */
	public static void writeWorkbook(XSSFWorkbook wb, String fileLoc) {
		try {
			FileOutputStream newExcelFile = new FileOutputStream(fileLoc);
			wb.write(newExcelFile);
			newExcelFile.close();  
		} catch (IOException e) {
			showMessage("Could not create file " + fileLoc + ".\nPlease check directory structure/permissions.");
			e.printStackTrace();
		}
	}
	
	public static Hashtable<String, Object> getParamsFromString(String params){
		Hashtable <String, Object> paramHash = new Hashtable<String, Object>();
		if(params != null)
		{
			StringTokenizer tokenz = new StringTokenizer(params,"~");
			while(tokenz.hasMoreTokens())
			{
				String thisToken = tokenz.nextToken();
				int index = thisToken.indexOf("$");
				String key = thisToken.substring(0, index);
				String value = thisToken.substring(index+1);
				// attempt to see if 
				boolean found = false;
				try{
					double dub = Double.parseDouble(value);
					paramHash.put(key, dub);
					found = true;
				}catch (Exception ignored)
				{
				}
				if(!found){
					try{
						int dub = Integer.parseInt(value);
						paramHash.put(key, dub);
						found = true;
					}catch (Exception ignored)
					{
					}
				}
				//if(!found)
					paramHash.put(key, value);
			}
		}
		return paramHash;
	}
	
	public static String retrieveResult(String api, Hashtable <String,String> params)
	{
		String output = "";
		try
		{
			URIBuilder uri = new URIBuilder(api);
			
			System.out.println("Getting data from the API...  " + api);
			System.out.println("Prams is " + params);
			
			SSLContextBuilder builder = new SSLContextBuilder();
		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
		            builder.build());
		    CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(
		            sslsf).build();

			HttpPost get = new HttpPost(api);
			if(params != null) // add the parameters
			{
				List <NameValuePair> nvps = new ArrayList <NameValuePair>();
				for(Enumeration <String> keys = params.keys();keys.hasMoreElements();)
				{
					String key = keys.nextElement();
					String value = params.get(key);
					uri.addParameter(key, value);
					nvps.add(new BasicNameValuePair(key, value));
				}
				get.setEntity(new UrlEncodedFormEntity(nvps));
				//get = new HttpPost(uri.build());
			}
			
			CloseableHttpResponse response = httpclient.execute(get);
			HttpEntity entity = response.getEntity();
			
			if(entity != null)
			{
				BufferedReader stream = new BufferedReader(new InputStreamReader(entity.getContent()));
				String data = null;
				while((data = stream.readLine()) != null)
					output = output + data;
			}
		}catch(Exception ex)
		{
			//connected = false;
		}
		if(output.length() == 0)
			output = null;
		
		return output;
	}

}
