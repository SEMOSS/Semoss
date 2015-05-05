/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;

/**
 * The Utility class contains a variety of miscellaneous functions implemented extensively throughout SEMOSS.
 * Some of these functionalities include getting concept names, printing messages, loading engines, and writing Excel workbooks.
 */
public class Utility {

	public static int id = 0;
	static Logger LOGGER = Logger.getLogger(prerna.util.Utility.class);

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
	 * Splits up a URI into tokens based on "/" character and uses logic to return the base URI
	 * @param String		URI to be split into tokens.
	 * @return String		Base URI */
	public static String getBaseURI(String uri) {
		int indexOf = uri.lastIndexOf("/");
		String baseURI = uri.substring(0, indexOf);
		
		indexOf = baseURI.lastIndexOf("/");
		baseURI = baseURI.substring(0, indexOf);
		
		indexOf = baseURI.lastIndexOf("/");
		baseURI = baseURI.substring(0, indexOf);
		
		return baseURI;
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

		ISelectWrapper sjw = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		sjw.setEngine(engine);
		sjw.setEngineType(engine.getEngineType());
		sjw.setQuery(query);
		sjw.executeQuery();*/
		String [] vars = sjw.getVariables();
		String returnType = null;
		while(sjw.hasNext()) {
			ISelectStatement stmt = sjw.next();
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


		ISelectWrapper selectWrapper = null;//WrapperManager.getInstance().getSWrapper(engine, queries.get(tabName));

		//SesameJenaSelectWrapper selectWrapper = null;
		for(String s : repos) {
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(s);
			selectWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			
			/*selectWrapper = new SesameJenaSelectWrapper();
			selectWrapper.setEngine(engine);
			selectWrapper.setQuery(query);
			selectWrapper.executeQuery();*/
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
	public static Integer showConfirm(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		return JOptionPane.showConfirmDialog(playPane, text);

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
		BigDecimal bigD = new BigDecimal(valueToRound);
		return bigD.setScale(numberOfDecimalPlaces, BigDecimal.ROUND_HALF_UP).doubleValue();
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
			if(prop.containsKey(Constants.DREAMER))
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
			if(prop.containsKey(Constants.ONTOLOGY))
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

	/**
	 * Cleans a string based on certain patterns
	 * @param 	Original string
	 * @param 	If true, replace forward slashes ("/") with dashes ("-")

	 * @return 	Cleaned string */
	public static String cleanString(String original, boolean replaceForwardSlash){
		return cleanString(original,replaceForwardSlash,true);
	}
	
	/**
	 * Cleans a string based on certain patterns
	 * @param 	Original string
	 * @param 	If true, replace forward slashes ("/") with dashes ("-")
	 * @param	replaceForRDF If true, replace double quote with single quote and replace space with underscore.  For RDBMS this should be false

	 * @return 	Cleaned string */
	public static String cleanString(String original, boolean replaceForwardSlash, boolean replaceForRDF){
		String retString = original;
		
		retString = retString.trim();
		retString = retString.replaceAll("\t", " ");//replace tabs with spaces
		while (retString.contains("  ")){
			retString = retString.replace("  ", " ");
		}
		retString = retString.replaceAll("\\{", "(");
		retString = retString.replaceAll("\\}", ")");
		retString = retString.replaceAll("\\\\", "-");//replace backslashes with dashes
		retString = retString.replaceAll("'", "");//remove apostrophe
		if(replaceForRDF){
			retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
			retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		}
		if(replaceForwardSlash) {
			retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
		}
		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		retString = retString.replaceAll("\n", " ");
		retString = retString.replaceAll("<", "(");
		retString = retString.replaceAll(">", ")");

		return retString;
	}
	
	public static String cleanVariableString(String original){
        String cleaned = cleanString (original, true);
        cleaned = cleaned.replaceAll("\\,", "");
        cleaned = cleaned.replaceAll("\\%", "");
        cleaned = cleaned.replaceAll("\\-", "");
        cleaned = cleaned.replaceAll("\\(", "");
        cleaned = cleaned.replaceAll("\\)", "");
        cleaned = cleaned.replaceAll("\\&", "and");
        return cleaned;
  }


	/**
	 * Creates an excel workbook
	 * @param wb 		XSSFWorkbook to write to
	 * @param fileLoc 	String containing the path to save the workbook
	 */
	public static void writeWorkbook(XSSFWorkbook wb, String fileLoc) {
		FileOutputStream newExcelFile = null;
		try {
			newExcelFile = new FileOutputStream(fileLoc);
			wb.write(newExcelFile);
			newExcelFile.flush();
		} catch (IOException e) {
			showMessage("Could not create file " + fileLoc + ".\nPlease check directory structure/permissions.");
			e.printStackTrace();
		}finally{
			try{
				if(newExcelFile!=null)
					newExcelFile.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
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
				}catch (RuntimeException ignored)
				{
					LOGGER.debug(ignored);
				}
				if(!found){
					try{
						int dub = Integer.parseInt(value);
						paramHash.put(key, dub);
					}catch (RuntimeException ignored)
					{
						LOGGER.debug(ignored);
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
		BufferedReader stream = null;
		InputStreamReader inputStream = null;
		CloseableHttpClient httpclient = null;
		try
		{
			URIBuilder uri = new URIBuilder(api);
			
			System.out.println("Getting data from the API...  " + api);
			System.out.println("Prams is " + params);
			
			SSLContextBuilder builder = new SSLContextBuilder();
		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
		            builder.build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
		    httpclient = HttpClients.custom().setSSLSocketFactory(
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
				inputStream = new InputStreamReader(entity.getContent());
				stream = new BufferedReader(inputStream);
				String data = null;
				while((data = stream.readLine()) != null)
					output = output + data;
			}
		}catch(RuntimeException ex)
		{
			LOGGER.debug(ex);
		} catch (IOException ex) {
			LOGGER.debug(ex);
		} catch (NoSuchAlgorithmException e) {
			LOGGER.debug(e);
		} catch (KeyStoreException e) {
			LOGGER.debug(e);
		} catch (URISyntaxException e) {
			LOGGER.debug(e);
		} catch (KeyManagementException e) {
			LOGGER.debug(e);
		} finally {
				try {
					if(inputStream!=null)
						inputStream.close();
					if(stream!=null)
						stream.close();
				} catch (IOException e) {
					LOGGER.error("Error closing input stream for image");
				}
				try {
					if(httpclient!=null)
						httpclient.close();
					if(stream!=null)
						stream.close();
				} catch (IOException e) {
					LOGGER.error("Error closing socket for httpclient");
				}
		}
		if(output.length() == 0)
			output = null;
		
		return output;
	}

	public static InputStream getStream(String api, Hashtable <String,String> params)
	{
		HttpEntity entity ;
		CloseableHttpClient httpclient = null;
		try
		{
			URIBuilder uri = new URIBuilder(api);
			
			System.out.println("Getting data from the API...  " + api);
			System.out.println("Prams is " + params);
			
			SSLContextBuilder builder = new SSLContextBuilder();
		    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
		            builder.build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
		    httpclient = HttpClients.custom().setSSLSocketFactory(
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
			entity = response.getEntity();
			return entity.getContent();
			
		}catch(RuntimeException ex)
		{
			//connected = false;
			ex.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
	
	public static ISelectWrapper processQuery(IEngine engine, String query) {
		LOGGER.info("PROCESSING QUERY: " + query);

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;*/
		return wrapper;
	}
	
	/**
	 * Determines the type of a given value
	 * @param s		The value to determine the type off
	 * @return		The type of the value
	 */
	public static String processType(String s) {
		
		if(s == null) {
			return null;
		}
		
		boolean isDouble = true;
		try {
			double val = Double.parseDouble(s);
			if(val == Math.floor(val)) {
				return "INTEGER";
			}
		} catch(NumberFormatException e) {
			isDouble = false;
		}

		if(isDouble) {
			return ("DOUBLE");
		}

		// will analyze date types as numerical data
		Boolean isLongDate = true;
		SimpleDateFormat formatLongDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
		try {
			formatLongDate.setLenient(true);
			formatLongDate.parse(s);
		} catch (ParseException e) {
			isLongDate = false;
		}
		if(isLongDate){
			return ("DATE");
		}

		Boolean isSimpleDate = true;
		SimpleDateFormat formatSimpleDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
		try {
			formatSimpleDate.setLenient(true);
			formatSimpleDate.parse(s);
		} catch (ParseException e) {
			isSimpleDate = false;
		}
		if(isSimpleDate){
			return ("SIMPLEDATE");
		}

		return ("STRING");
	}

	public static String[] filterNames(String[] names, boolean[] include) {
		int size = 0;
		for(boolean val : include) {
			if(val) {
				size++;
			}
		}

		String[] newNames = new String[size];
		int nextIndex=0;
		for(int i=0;i<names.length;i++) {
			if(include[i]) {
				newNames[nextIndex]=names[i];
				nextIndex++;
			}
		}
		return newNames;
	}
	
	public static Date getCurrentTime() {
		Calendar calendar = Calendar.getInstance();
		TimeZone timeZone = calendar.getTimeZone();
		calendar.setTimeZone(timeZone);
		
		return calendar.getTime();
	}
	
}
