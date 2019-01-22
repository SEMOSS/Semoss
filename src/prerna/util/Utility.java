/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.commons.io.FilenameUtils;
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
import org.apache.poi.ss.usermodel.Workbook;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.AZClient;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.ZKClient;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

/**
 * The Utility class contains a variety of miscellaneous functions implemented extensively throughout SEMOSS.
 * Some of these functionalities include getting concept names, printing messages, loading engines, and writing Excel workbooks.
 */
public class Utility {

	public static int id = 0;
	static Logger LOGGER = Logger.getLogger(prerna.util.Utility.class);

	private static ConcurrentMap<String, ReentrantLock> engineLocks = new ConcurrentHashMap<String, ReentrantLock>();
	
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
			LOGGER.debug(data);
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

			LOGGER.debug(data);
			// put something to strip the @
			paramHash.put(paramName, paramValue);
		}

		return paramHash;
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	public static String unescapeHTML(String s) {
		s = s.replaceAll("&gt;", ">");
	    s = s.replaceAll("&lt;", "<");
	    s = s.replaceAll("&#61;", "=");
	    s = s.replaceAll("&#33;", "!" );
	    return s;
	}
	
	/**
	 * Matches the given query against a specified pattern.
	 * While the next substring of the query matches a part of the pattern, set substring as the key with EMPTY constants (@@) as the value
	 * @param 	Query.

	 * @return 	Hashtable of queries to be replaced */
	public static String normalizeParam(String query)	{	
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();		
		Pattern pattern = Pattern.compile("[@]{1}\\w+[-]*[\\w/.:]+[@]");

		Matcher matcher = pattern.matcher(query);
		while(matcher.find()) {
			String data = matcher.group();
			data = data.substring(1,data.length()-1);
			if(data.contains("-")) {
				String paramName = data.substring(0, data.indexOf("-"));
				String paramValue = data.substring(data.indexOf("-") + 1);

				LOGGER.debug(data);
				// put something to strip the @
				List<Object> retList = new ArrayList<Object>();
				retList.add("@"+ paramName + "@");
				paramHash.put(data, retList);
			}
		}

		return fillParam(query, paramHash);
	}


	/**
	 * Gets the param hash and replaces certain queries
	 * @param 	Original query
	 * @param 	Hashtable of format [String to be replaced] [Replacement]

	 * @return 	If applicable, returns the replaced query */
	public static String fillParam(String query, Map<String, List<Object>> paramHash) {
		// NOTE: this process always assumes only one parameter is selected
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		LOGGER.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while(keys.hasNext()) {
			String key = (String)keys.next();
			String value = paramHash.get(key).get(0) + "";
			LOGGER.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if(!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}

		return query;
	}
	
	/**
	 * Gets the param hash and replaces certain queries
	 * @param 	Original query
	 * @param 	Hashtable of format [String to be replaced] [Replacement]

	 * @return 	If applicable, returns the replaced query */
	public static String fillParam2(String query, Map<String, String> paramHash) {
		// NOTE: this process always assumes only one parameter is selected
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		LOGGER.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while(keys.hasNext()) {
			String key = (String)keys.next();
			String value = paramHash.get(key);
			LOGGER.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
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
	 * Splits up a URI into tokens based on "/" character and uses logic to return the node instance name for a display name of a property 
	 * where the node's display name is prior to the property display name.
	 * @param String		URI to be split into tokens.
	 * @return String		Instance name. */
	public static String getInstanceNodeName(String uri) {
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens() - 1;
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
	 * Splits up a URI into tokens based on "/" character and uses logic to return the primary key.
	 * @param String		URI to be split into tokens. (BASE_URI/Concept/PRIMARY_KEY/INSTANCE_NAME

	 * @return String		Primary Key
	 * */
	public static String getPrimaryKeyFromURI(String uri) {
		String[] elements = uri.split("/");
		if(elements.length >= 2) {
			return elements[elements.length-2];
		} else {
			return uri;
		}
	}

	public static String getFQNodeName(IEngine engine, String URI) {
		if(engine.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)) {
			return getInstanceName(URI) + "__" + getPrimaryKeyFromURI(URI);
		} else {
			return getInstanceName(URI);
		}
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
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
		List<Object> values = new ArrayList<Object>();
		values.add(subjectURI);
		paramHash.put("ENTITY", values);
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

	//	public static void main(String[] args) {
	//		String date = "qweqweqw";
	//		System.out.println(isStringDate(date));
	//	}

	public static boolean isStringDate(String inDate) {
		List<SimpleDateFormat> dateFormatList = new ArrayList<SimpleDateFormat>();

		dateFormatList.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'"));
		dateFormatList.add(new SimpleDateFormat("MM-dd-yyyy__HH:mm:ss_a"));

		for(SimpleDateFormat format : dateFormatList){
			try {
				format.setLenient(false);
				format.parse(inDate);
				return true;
			} catch (ParseException e) {

			}
		}
		return false;
	}
	
	public static boolean isFactorType(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.startsWith("FACTOR")) {
			return true;
		}

		return false;
	}

	/**
	 * Changes a value within the SMSS file for a given key
	 * @param smssPath					The path to the SMSS file
	 * @param keyToAlter				The key to alter
	 * @param valueToProvide			The value to give the key
	 */
	public static void changePropMapFileValue(String smssPath, String keyToAlter, String valueToProvide) {
		changePropMapFileValue(smssPath, keyToAlter, valueToProvide, false);
	}
	
	public static void changePropMapFileValue(String smssPath, String keyToAlter, String valueToProvide, boolean contains) {
		FileOutputStream fileOut = null;
		File file = new File(smssPath);

		/*
		 * 1) Loop through the smss file and add each line as a list of strings
		 * 2) For each line, see if it starts with the key to alter
		 * 3) if yes, write out the key with the new value passed in
		 * 4) if no, just write out the line as is
		 * 
		 */
		List<String> content = new ArrayList<String>();
		BufferedReader reader = null;
		FileReader fr = null;
		try{
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			// 1) add each line as a different string in list
			while((line = reader.readLine()) != null){
				content.add(line);
			}

			fileOut = new FileOutputStream(file);
			byte[] lineBreak = "\n".getBytes();
			// 2) iterate through each line if the smss file
			for(int i=0; i<content.size(); i++){
				// 3) if this line starts with the key to alter
				
				if(contains) {
					if(content.get(i).contains(keyToAlter)) {
						// create new line to write using the key and the new value
						String newKeyValue = keyToAlter + "\t" + valueToProvide;
						fileOut.write(newKeyValue.getBytes());
					}
					
					// 4) if it doesn't, just write the next line as is
					else {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				} else {
					if(content.get(i).startsWith(keyToAlter + "\t") || content.get(i).startsWith(keyToAlter + " ")){
						// create new line to write using the key and the new value
						String newKeyValue = keyToAlter + "\t" + valueToProvide;
						fileOut.write(newKeyValue.getBytes());
					}
					
					// 4) if it doesn't, just write the next line as is
					else {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				}
			}
		} catch(IOException e){
			e.printStackTrace();
		} finally{
			// close the readers
			try{
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try{
				fileOut.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * Adds a new key-value pair into the SMSS file
	 * @param smssPath					The path of the smss file
	 * @param keyToAdd					The key to add into the smss file
	 * @param valueToProvide			The value for the key to add to the smss file
	 */
	public static void updateSMSSFile(String smssPath, String keyToAdd, String valueToProvide) {
		updateSMSSFile(smssPath, "OWL", keyToAdd, valueToProvide);
	}
	
	public static void updateSMSSFile(String smssPath, String locInFile, String keyToAdd, String valueToProvide) {
		FileOutputStream fileOut = null;
		File file = new File(smssPath);

		/*
		 * 1) Loop through the smss file and add each line as a list of strings
		 * 2) iterate through the list of strings and write out each line
		 * 3) if the current line being printed starts with locInFile (hard coded as OWL)
		 * 		then the new key-value pair will be written right after it
		 */

		List<String> content = new ArrayList<String>();
		BufferedReader reader = null;
		FileReader fr = null;
		try{
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			// 1) add each line as a different string in list
			while((line = reader.readLine()) != null){
				content.add(line);
			}

			fileOut = new FileOutputStream(file);
			for(int i=0; i<content.size(); i++){
				// 2) write out each line into the file
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());

				// 3) if the last line printed matches that in locInFile, then write the new
				// 		key-value pair after
				if(content.get(i).startsWith(locInFile + "\t") || content.get(i).startsWith(locInFile + " ")){
					String newProp = keyToAdd + "\t" + valueToProvide;
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
		} catch(IOException e){
			e.printStackTrace();
		} finally{
			// close the readers
			try{
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try{
				fileOut.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * Cleans a string based on certain patterns
	 * @param 	Original string
	 * @param 	If true, replace forward slashes ("/") with dashes ("-")

	 * @return 	Cleaned string */
	public static String cleanString(String original, boolean replaceForwardSlash){
		return cleanString(original,replaceForwardSlash,true, false);
	}

	/**
	 * Cleans a string based on certain patterns
	 * @param 	Original string
	 * @param 	If true, replace forward slashes ("/") with dashes ("-")
	 * @param	replaceForRDF If true, replace double quote with single quote and replace space with underscore.  For RDBMS this should be false

	 * @return 	Cleaned string */
	public static String cleanString(String original, boolean replaceForwardSlash, boolean replaceForRDF, boolean property){
		String retString = original;

		retString = retString.trim();
		retString = retString.replaceAll("\t", " ");//replace tabs with spaces
		while (retString.contains("  ")){
			retString = retString.replace("  ", " ");
		}
		retString = retString.replaceAll("\\{", "(");
		retString = retString.replaceAll("\\}", ")");
		retString = retString.replaceAll("'", "");//remove apostrophe
		if(replaceForRDF){
			retString = retString.replaceAll("\"", "'");//replace double quotes with single quotes
		}
		retString = retString.replaceAll(" ", "_");//replace spaces with underscores
		if(!property) {
			if(replaceForwardSlash) {
				retString = retString.replaceAll("/", "-");//replace forward slashes with dashes
			}
			retString = retString.replaceAll("\\\\", "-");//replace backslashes with dashes
		}

		retString = retString.replaceAll("\\|", "-");//replace vertical lines with dashes
		retString = retString.replaceAll("\n", " ");
		retString = retString.replaceAll("<", "(");
		retString = retString.replaceAll(">", ")");

		return retString;
	}

	public static String cleanVariableString(String original){
		String cleaned = cleanString (original, true);
		cleaned = cleaned.replace(",", "");
		cleaned = cleaned.replace("%", "");
		cleaned = cleaned.replace("-", "");
		cleaned = cleaned.replace("(", "");
		cleaned = cleaned.replace(")", "");
		cleaned = cleaned.replace("&", "and");
		while(cleaned.contains("__")) {
			cleaned = cleaned.replace("__", "_");
		}

		return cleaned;
	}
	
	public static String makeAlphaNumeric(String s) {
		s = s.trim();
		s = s.replaceAll(" ", "_");
		s = s.replaceAll("[^a-zA-Z0-9\\_]", "");
		while(s.contains("__")){
			s = s.replace("__", "_");
		}
		return s;
	}

	public static String cleanPredicateString(String original){
		String cleaned = cleanString (original, true);
		cleaned = cleaned.replaceAll("[()]", "");
		cleaned = cleaned.replace(",", "");
		cleaned = cleaned.replace("?", "");
		cleaned = cleaned.replace("&", "");
		return cleaned;
	}

	/**
	 * Creates an excel workbook
	 * @param wb 		XSSFWorkbook to write to
	 * @param fileLoc 	String containing the path to save the workbook
	 */
	public static void writeWorkbook(Workbook wb, String fileLoc) {
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

			LOGGER.debug("Getting data from the API...  " + api);
			LOGGER.debug("Params is " + params);

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

			LOGGER.info("Getting data from the API...  " + api);
			LOGGER.info("Prams is " + params);

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
		LOGGER.debug("PROCESSING QUERY: " + query);

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

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * @param raw TODO
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String> getVectorOfReturn(String query, IEngine engine, Boolean raw){
		Vector<String> retArray = new Vector<String>();
		IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(engine, query);

		while(wrap.hasNext()) {
			Object[] values = null;
			if(raw) {
				values = wrap.next().getRawValues();
			} else {
				values = wrap.next().getValues();
			}
			
			if(values[0] != null) {
				retArray.add(values[0].toString());
			} else {
				retArray.add(null);
			}
		}
		return retArray;
	}

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * @param raw TODO
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String[]> getVectorArrayOfReturn(String query, IEngine engine, Boolean raw){
		Vector<String[]> retArray = new Vector<String[]>();
		IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(engine, query);

		while(wrap.hasNext()) {
			Object[] values = null;
			if(raw) {
				values = wrap.next().getRawValues();
			} else {
				values = wrap.next().getValues();
			}
			
			String[] valArray = new String[values.length];
			for(int i = 0; i < values.length; i++) {
				if(values[i] != null) {
					valArray[i] = values[i] + "";
				}
			}
			retArray.add(valArray);
		}
		return retArray;
	}

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String[]> getVectorObjectOfReturn(String query,IEngine engine){
		Vector<String[]> retString = new Vector<String[]>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(engine, query);

		String[] names = wrap.getPhysicalVariables();


		while (wrap.hasNext()) {
			String[] values = new String[names.length];
			ISelectStatement bs = wrap.next();
			for(int i = 0; i < names.length; i++){
				Object value = bs.getRawVar(names[i]);
				String val = null;
				if(value instanceof Binding){
					val = ((Value)((Binding) value).getValue()).stringValue();
				}
				else{
					val = value +"";
				}
				val = val.replace("\"", "");
				values[i] = val;
			}
			retString.addElement(values);
		}
		return retString;

	}

	public static Map<String, List<Object>> cleanParamsForRDBMS(Map<String, List<Object>> paramHash) {
		// TODO Auto-generated method stub
		// really simple, I am runnign the keys and then I will have strip the instance values out
		Iterator <String> keys = paramHash.keySet().iterator();
		while(keys.hasNext())
		{
			String singleKey = keys.next();
			List<Object> valueList = paramHash.get(singleKey);
			List<Object> newValuesList = new Vector<Object>();
			for(int i = 0; i < valueList.size(); i++) {
				String value = valueList.get(i) + "";
				if(value.startsWith("http:") || value.contains(":")) {
					value = getInstanceName(value);
				}
				newValuesList.add(value);
			}
			paramHash.put(singleKey, newValuesList);
		}

		return paramHash;
	}

	public static IPlaySheet getPlaySheet(IEngine engine, String psName){
		LOGGER.info("Trying to get playsheet for " + psName);
		String psClassName = null;
		if(engine != null) {
			psClassName = engine.getProperty(psName);
		}
		if(psClassName == null){
			psClassName = (String) DIHelper.getInstance().getLocalProp(psName);
		}
		if(psClassName == null){
			psClassName = (String) DIHelper.getInstance().getProperty(psName);
		}
		if(psClassName == null){
			psClassName = PlaySheetRDFMapBasedEnum.getClassFromName(psName);
		}
		if(psClassName == null || psClassName.isEmpty()){
			psClassName = psName;
		}

		IPlaySheet playSheet = (IPlaySheet) getClassFromString(psClassName);

		return playSheet;
	}

	public static IDataMaker getDataMaker(IEngine engine, String dataMakerName){
		LOGGER.info("Trying to get data maker for " + dataMakerName);
		String dmClassName = null;
		if(engine != null) {
			dmClassName = engine.getProperty(dataMakerName);
		}
		if(dmClassName == null){
			dmClassName = (String) DIHelper.getInstance().getLocalProp(dataMakerName);
		}
		if(dmClassName == null){
			dmClassName = (String) DIHelper.getInstance().getProperty(dataMakerName);
		}
		if(dmClassName == null){
			dmClassName = PlaySheetRDFMapBasedEnum.getClassFromName(dataMakerName);
		}
		if(dmClassName == null || dmClassName.isEmpty()){
			dmClassName = dataMakerName;
		}

		IDataMaker dm = (IDataMaker) getClassFromString(dmClassName);

		return dm;
	}

	public static IPlaySheet preparePlaySheet(IEngine engine, String sparql, String psName, String playSheetTitle, String insightID)
	{
		IPlaySheet playSheet = getPlaySheet(engine, psName);
		//		QuestionPlaySheetStore.getInstance().put(insightID,  playSheet);
		playSheet.setQuery(sparql);
		playSheet.setRDFEngine(engine);
		playSheet.setQuestionID(insightID);
		playSheet.setTitle(playSheetTitle);
		return playSheet;
	}

	public static ISEMOSSTransformation getTransformation(IEngine engine, String transName){
		LOGGER.info("Trying to get transformation for " + transName);
		String transClassName = (String) DIHelper.getInstance().getLocalProp(transName);
		if(transClassName == null){
			transClassName = (String) DIHelper.getInstance().getProperty(transName);
		}
		if(transClassName == null || transClassName.isEmpty()){
			transClassName = transName;
		}

		ISEMOSSTransformation transformation = (ISEMOSSTransformation) getClassFromString(transClassName);
		return transformation;
	}

	public static ISEMOSSAction getAction(IEngine engine, String actionName){
		LOGGER.info("Trying to get action for " + actionName);
		String actionClassName = (String) DIHelper.getInstance().getLocalProp(actionName);
		if(actionClassName == null){
			actionClassName = (String) DIHelper.getInstance().getProperty(actionName);
		}
		if(actionClassName == null || actionClassName.isEmpty()){
			actionClassName = actionName;
		}

		ISEMOSSAction action = (ISEMOSSAction) getClassFromString(actionClassName);
		return action;
	}

	public static Object getClassFromString(String className){
		Object obj = null;
		try {
			System.out.println("Dataframe name is " + className);
			obj = Class.forName(className).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			LOGGER.fatal("No such class: "+ className);
		} catch (InstantiationException e) {
			e.printStackTrace();
			LOGGER.fatal("Failed instantiation: "+ className);
		} catch (IllegalAccessException e) {
			LOGGER.fatal("Illegal Access: "+ className);
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			LOGGER.fatal("Illegal argument: "+ className);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			LOGGER.fatal("Invoation exception: "+ className);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			LOGGER.fatal("No constructor: "+ className);
			e.printStackTrace();
		} catch (SecurityException e) {
			LOGGER.fatal("Security exception: "+ className);
			e.printStackTrace();
		}
		return obj;
	}

	public static String getKeyFromValue(Map<String, String> hm, String value){
		for (String o : hm.keySet()) {
			if (hm.get(o).equals(value)) {
				return o;
			}
		}
		return null;
	}

//	/**
//	 * 
//	 * @param engine
//	 * @param nodesList
//	 * @param getDisplayNames
//	 * @return
//	 */
//	public static List getTransformedNodeNamesList(IEngine engine, List nodesList, boolean getDisplayNames){
//		//array, list or vector support only
//
//		for(int i = 0; i< nodesList.size(); i++){
//			String currentUri =  nodesList.get(i).toString();
//			String finalUri = Utility.getTransformedNodeName(engine, currentUri , getDisplayNames);
//			if(!finalUri.equals(currentUri)){
//				nodesList.set(i, finalUri);
//			}
//		}
//		return nodesList;
//	}

//	/**
//	 *  use for maps that map a string to a list of nodes...more commmonly used than I expected
//	 * @param engine
//	 * @param nodeMap
//	 * @param getDisplayNames
//	 * @return
//	 */
//	public static Map<String, List<Object>> getTransformedNodeNamesMap(IEngine engine, Map<String, List<Object>> nodeMap, boolean getDisplayNames){
//		if(nodeMap!= null && nodeMap.size()>0){
//			for(Map.Entry<String, List<Object>> eachMap: nodeMap.entrySet()){
//				List<Object> binding = Utility.getTransformedNodeNamesList(engine, nodeMap.get(eachMap.getKey()),false);
//			}
//		}
//		return nodeMap;
//	}


//	/**
//	 * returns the physical or logical/display name
//	 * loop through the first time using the qualified class name ie this assumes you got something like http://semoss.org/ontologies/Concept/Director/Clint_Eastwood
//	 *     so getting the qualified name will give you http://semoss.org/ontologies/Concept/Director
//	 * if the translated uri is the same (ie no translated name was found, so maybe you actually did come into this method with a nodeUri of http://semoss.org/ontologies/Concept/Director) 
//	 *     on the second loop use that nodeUri directly and try to find a translated Uri
//	 * 
//	 * @param engine
//	 * @param nodeUri
//	 * @param getDisplayNames
//	 * @return
//	 */
//	public static String getTransformedNodeName(IEngine engine, String nodeUri, boolean getDisplayNames){
//		String finalUri = nodeUri;
//		boolean getClassName = true;
//		for(int j = 0; j < 2; j++){
//
//			String fullUri = nodeUri;
//			String uri = fullUri;
//
//			if(getClassName){
//				uri = Utility.getQualifiedClassName(uri);
//				getClassName = false;
//			}
//			String physicalUri = engine.getTransformedNodeName(uri, getDisplayNames);
//			if(!uri.equals(physicalUri)){
//				finalUri = fullUri.replace(uri, physicalUri);
//				break;
//			}
//		}
//		return finalUri;
//	}

	/**
	 * Return Object[] with prediction of the data type
	 * Index 0 -> return the casted object
	 * Index 1 -> return the pixel data type
	 * Index 2 -> return the additional formatting
	 * @param input
	 * @return
	 */
	public static Object[] determineInputType(String input) {
		Object [] retObject = new Object[3];
		if(input != null) {
			Object retO = null;
			// is it a boolean ?
			if(input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
				retObject[0] = Boolean.parseBoolean(input);
				retObject[1] = SemossDataType.BOOLEAN;
			}
			// is it a date ?
			else if( (retO = SemossDate.genDateObj(input)) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.DATE;
				retObject[2] = ((SemossDate) retO).getPattern();
			}
			// is it a timestamp ?
			else if( (retO = SemossDate.genTimeStampDateObj(input)) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.TIMESTAMP;
				retObject[2] = ((SemossDate) retO).getPattern();
			}
			// is it an integer ?
			else if( (retO = getInteger(input.replaceAll("[^0-9\\.E]", ""))) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.INT;
			}
			// is it a double ?
			else if( (retO = getDouble(input.replaceAll("[^0-9\\.E]", ""))) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.DOUBLE;
			}
			// well, i guess it is a string
			else {
				retObject[0] = input;
				retObject[1] = SemossDataType.STRING;
			}
		}
		return retObject;
	}
	
	@Deprecated
	public static Object[] findTypes(String input)
	{
		//System.out.println("String that came in.. " + input);
		Object [] retObject = null;
		if(input != null)
		{
			Object retO = null;
			if(input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false"))
			{
				retObject = new Object[2];
				retObject[0] = "boolean";
				retObject[1] = retO;

			}
			//all numbers are 
			//	    	else if(NumberUtils.isDigits(input))
			//	    	{
			//	    		retO = Integer.parseInt(input);
			//	    		retObject = new Object[2];
			//	    		retObject[0] = "int";
			//	    		retObject[1] = retO;
			//	    	}
			
			else if((retO = getDate(input)) != null )// try dates ? - yummy !!
			{
				retObject = new Object[2];
				retObject[0] = "date";
				retObject[1] = retO;

			}
			else if((retO = getDouble(input)) != null )
			{
				retObject = new Object[2];
				retObject[0] = "double";
				retObject[1] = retO;
			}
			else if((retO = getCurrency(input)) != null )
			{

				retObject = new Object[2];
				if(retO instanceof String)
					retObject[0] = "varchar(800)";
				else
					retObject[0] = "double";
				retObject[1] = retO;
			}
			else
			{
				retObject = new Object[2]; // need to do some more stuff to determine this
				retObject[0] = "varchar(800)";
				retObject[1] = input; 
			}
		}
		return retObject;
	}

	@Deprecated
	public static String getDate(String input)
	{
		String[] date_formats = {
				// year, month, day
				"yyyy-MM-dd",
				"yyyy-MM-d",
				"yyyy-M-dd",
				"yyyy-M-d",
				// day, month, year
				"dd-MM-yyyy",
				"d-MM-yyyy",
				"dd-M-yyyy",
				"d-M-yyyy",
				// year / month / day
				"yyyy/MM/dd",
				"yyyy/MM/d",
				"yyyy/M/dd",
				"yyyy/M/d",
				// day, month, year
				"dd/MM/yyyy",
				"d/MM/yyyy",
				"dd/M/yyyy",
				"d/M/yyyy",

				// Abrev. month, day, year
				"MMM_dd,_yyyy",
				"MMM_d,_yyyy",
				"MMM dd, yyyy",
				"MMM d, yyyy",

				//"dd/MM/yyyy",
				"MM/dd/yyyy",
				"yyyy/MM/dd", 
				"yyyy MMM dd",
				"yyyy dd MMM",
				"dd MMM yyyy",
				"dd MMM",
				"MMM dd yyyy",
				"MMM dd, yyyy",
				"MMM dd",
				"dd MMM yyyy",
				"MMM yyyy",
				"dd/MM/yyyy"
				};

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats)
		{
			try
			{
				SimpleDateFormat sdf = new SimpleDateFormat(formatString);
				Date mydate = sdf.parse(input);
				SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd");
				output_date = outdate.format(mydate);
				itsDate = true;
				break;
			}
			catch (ParseException e) {
				//System.out.println("Next!");
			}
		}
		
		return output_date;	
	}

	@Deprecated
	public static String getTimeStamp(String input)
	{
		String[] date_formats = {
				// year, month, day
				"yyyy-MM-dd hh:mm:ss",
				"yyyy-MM-d hh:mm:ss",
				"yyyy-M-dd hh:mm:ss",
				"yyyy-M-d hh:mm:ss",
				"yyyy-MM-dd'T'hh:mm:ss'Z'",
				"yyyy-MM-d'T'hh:mm:ss'Z'",
				"yyyy-M-dd'T'hh:mm:ss'Z'",
				"yyyy-M-d'T'hh:mm:ss'Z'",
				};

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats)
		{
			try
			{    
				Date mydate = new SimpleDateFormat(formatString).parse(input);
				SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				output_date = outdate.format(mydate);
				itsDate = true;
				break;
			}
			catch (ParseException e) {
				//System.out.println("Next!");
			}
		}

		return output_date;	
	}

	@Deprecated
	public static Date getDateAsDateObj(String input) {
		SimpleDateFormat outdate_formatter = new SimpleDateFormat("yyyy-MM-dd");
		String output_date = getDate(input);
		if(output_date == null) {
			return null;
		}

		Date outDate = null;
		try {
			outDate = outdate_formatter.parse(output_date);
		} catch (ParseException e) {
//			e.printStackTrace();
		}

		return outDate;
	}
	
	@Deprecated
	public static Date getDateObjFromStringFormat(String input, String curFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(curFormat);
		Date mydate = null;
		try {
			mydate = sdf.parse(input);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return mydate;
	}
	
	@Deprecated
	public static Date getTimeStampAsDateObj(String input) {
		SimpleDateFormat outdate_formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.ssss");
		String output_date = getTimeStamp(input);
		if(output_date == null) {
			return null;
		}

		Date outDate = null;
		try {
			outDate = outdate_formatter.parse(output_date);
		} catch (ParseException e) {
//			e.printStackTrace();
		}

		return outDate;
	}
	
	@Deprecated
	public static Object getCurrency(String input)
	{
		//COMMENTING THIS OUT BECAUSE CAST TO TYPES BREAKS IN CASES WHERE THIS RETURNS, NEED TO UPDATE THAT BUT WILL KEEP IT AS STRING FOR NOW
		// what is this check??? 
		// this is messing up the types since it works based on if there is a null pointer
		//    	if(input.matches("\\Q$\\E(\\d+)\\Q.\\E?(\\d+)?\\Q-\\E\\Q$\\E(\\d+)\\Q.\\E?(\\d+)?")) {
		//    		return input;
		//    	}
		//    	Number nm = null;
		//    	NumberFormat nf = NumberFormat.getCurrencyInstance();
		//    	try {
		//    		nm = nf.parse(input);
		//    		//System.out.println("Curr..  " + nm);
		//    	}catch (Exception ex)
		//    	{
		//    		
		//    	}
		//    	return nm;
		return null;
	}

	public static Double getDouble(String input) {
		// try to do some basic clean up if it fails and try again
		try {
			Double num = Double.parseDouble(input);
			return num;
		} catch(NumberFormatException e) {
			return null;
		}
	}
	
	public static Integer getInteger(String input) {
		// try to do some basic clean up if it fails and try again
		try {
			Integer num = Integer.parseInt(input);
			return num;
		} catch(NumberFormatException e) {
			Double db = getDouble(input);
			if(db != null && db == db.intValue()) {
				return db.intValue();
			}
			return null;
		}
	}

	//this doesn't consider 1.2E8 etc.
	//    public static boolean isNumber(String input) {
	//    	//has digits, followed by optional period followed by digits
	//    	return input.matches("(\\d+)\\Q.\\E?(\\d+)?"); 
	//    }

	@Deprecated
	public static String[] castToTypes(String [] thisOutput, String [] types)
	{
		String [] values = new String[thisOutput.length];

		//System.out.println("OUTPUT"  + thisOutput);
		//System.out.println("TYPES" +  types);


		for(int outIndex = 0;outIndex < thisOutput.length;outIndex++)
		{
			//System.out.println(" Data [" + thisOutput[outIndex] + "]  >> [" + types[outIndex] + "]");
			//if the value is not null
			if(thisOutput[outIndex] != null && thisOutput[outIndex].length() > 0)
			{
				values[outIndex] = thisOutput[outIndex] + "";

				if(thisOutput[outIndex] != null) // && castTargets.contains(outIndex + ""))
				{
					if(types[outIndex].equalsIgnoreCase("Date"))
						values[outIndex] = getDate(thisOutput[outIndex]);
					else if(types[outIndex].equalsIgnoreCase("Currency"))// this is a currency
						values[outIndex] = getCurrency(thisOutput[outIndex])+"";
					else if(types[outIndex].equalsIgnoreCase("varchar(800)"))
					{
						if(thisOutput[outIndex].length() >= 800)
							thisOutput[outIndex] = thisOutput[outIndex].substring(0,798);
						values[outIndex] = thisOutput[outIndex];
					}
				}
			}
			else if(types[outIndex] != null)
			{
				if(types[outIndex].equalsIgnoreCase("Double"))
					values[outIndex] = "NULL";
				else if(types[outIndex].equalsIgnoreCase("varchar(800)")|| types[outIndex].equalsIgnoreCase("date"))
					values[outIndex] = "";
			}
			else
			{
				values[outIndex] = "";
			}
		}

		//    	for(int i = 0; i < values.length; i++) {
		//    		values[i] = UriEncoder.encode(values[i]);
		//    	}

		for(int i = 0; i < values.length; i++) {
			values[i] = Utility.cleanString(values[i], true, true, false);
		}
		return values;
	}

	@Deprecated
	public static String castToTypes(String thisOutput, String type)
	{
		String values = "";

		//System.out.println("OUTPUT"  + thisOutput);
		//System.out.println("TYPES" +  types);


		if(thisOutput != null && thisOutput.length() > 0)
		{
			values = thisOutput + "";

			if(thisOutput != null) // && castTargets.contains(outIndex + ""))
			{
				if(type.equalsIgnoreCase("Date"))
					values = getDate(thisOutput);
				else if(type.equalsIgnoreCase("Currency"))// this is a currency
					values = getCurrency(thisOutput) + "";
				else if(type.equalsIgnoreCase("varchar(800)"))
				{
					if(thisOutput.length() >= 800)
						thisOutput = thisOutput.substring(0,798);
					values = thisOutput;
				}
			}
		}
		else if(type != null)
		{
			if(type.equalsIgnoreCase("Double"))
				values = "NULL";
			else if(type.equalsIgnoreCase("varchar(800)") || type.equalsIgnoreCase("date"))
				values = "";
		}
		else
		{
			values = "";
		}
		return values;
	}

	public static Map<Integer, Set<Integer>> getCardinalityOfValues(String[] newHeaders, Map<String, Set<String>> edgeHash) {
		Map<Integer, Set<Integer>> retMapping = new Hashtable<Integer, Set<Integer>>();

		if(edgeHash == null) {
			return retMapping;
		}

		for(String startNode : edgeHash.keySet()) {
			Integer startIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(newHeaders, startNode);

			// for nulls and stuff
			Set<String> set = edgeHash.get(startNode);
			if(set==null) {
				continue;
			}

			// finish the mappings
			for(String endNode : set) {
				Integer endIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(newHeaders, endNode);

				// add mapping
				if(!retMapping.containsKey(startIndex)) {
					Set<Integer> downstream = new HashSet<Integer>();
					downstream.add(endIndex);
					retMapping.put(startIndex, downstream);
				} else {
					Set<Integer> downstream = retMapping.get(startIndex);
					downstream.add(endIndex);
					retMapping.put(startIndex, downstream);
				}
			}
		}
		return retMapping;
	}

	public static String getRawDataType(String cleanDataType) {
		if(cleanDataType == null || cleanDataType.isEmpty()) {
			return "VARCHAR(800)";
		}
		cleanDataType = cleanDataType.toUpperCase();

		if(cleanDataType.equals("STRING")) {
			return "VARCHAR(800)";
		}

		// currently send double and date, which are the same as raw data type
		return cleanDataType;
	}

	public static String getCleanDataType(String origDataType) {
		if(origDataType == null || origDataType.isEmpty()) {
			return "STRING";
		}
		origDataType = origDataType.toUpperCase();

		if(isIntegerType(origDataType) || isDoubleType(origDataType)) {
			return "NUMBER";
		}

		if(isDateType(origDataType) || isTimeStamp(origDataType)) {
			return "DATE";
		}

		if(isStringType(origDataType)) {
			return "STRING";
		}

		return "STRING";
	}

	public static String getH2DataType(String dataType) {
		if(isH2DataType(dataType)) {
			return dataType;
		}
		
		String returnType = getH2TypeConversionMap().get(dataType);
		
		return returnType;
	}
	
	public static boolean isH2DataType(String dataType) {
		if(
				//INT TYPE
				dataType.equals("INT")
				|| dataType.equals("INTEGER")
				|| dataType.equals("MEDIUMINT")
				|| dataType.equals("INT4")
				|| dataType.equals("SIGNED")

				//BOOLEAN TYPE
				|| dataType.equals("BOOLEAN")
				|| dataType.equals("BIT")
				|| dataType.equals("BOOL")

				//TINYINT TYPE
				|| dataType.equals("TINYINT")

				//SMALLINT TYPE
				|| dataType.equals("SMALLINT")
				|| dataType.equals("INT2")
				|| dataType.equals("YEAR")

				//BIGINT TYPE
				|| dataType.equals("BIGINT")
				|| dataType.equals("INT8")

				//IDENTITY TYPE
				|| dataType.equals("IDENTITY")

				//DECIMAL TYPE
				|| dataType.equals("DECIMAL")
				|| dataType.equals("NUMBER")
				|| dataType.equals("DEC")
				|| dataType.equals("NUMERIC")

				//DOUBLE TYPE
				|| dataType.equals("DOUBLE")
				|| dataType.equals("PRECISION")
				|| dataType.equals("FLOAT")
				|| dataType.equals("FLOAT8")

				//REAL TYPE
				|| dataType.equals("REAL")
				|| dataType.equals("FLOAT4")

				//TIME TYPE
				|| dataType.equals("TIME")

				//DATE TYPE
				|| dataType.equals("DATE")

				//TIMESTAMP TYPE
				|| dataType.equals("TIMESTAMP")
				|| dataType.equals("DATETIME")
				|| dataType.equals("SMALLDATETIME")

				//BINARY TYPE
				|| dataType.startsWith("BINARY")
				|| dataType.startsWith("VARBINARY")
				|| dataType.startsWith("LONGVARBINARY")
				|| dataType.startsWith("RAW")
				|| dataType.startsWith("BYTEA")

				//OTHER TYPE
				|| dataType.equals("OTHER")

				//VARCHAR TYPE
				|| dataType.startsWith("VARCHAR")
				|| dataType.startsWith("LONGVARCHAR")
				|| dataType.startsWith("VARCHAR2")
				|| dataType.startsWith("NVARCHAR")
				|| dataType.startsWith("NVARCHAR2")
				|| dataType.startsWith("VARCHAR_CASESENSITIVE")

				//VARCHAR_IGNORECASE TYPE
				|| dataType.startsWith("VARCHAR_IGNORECASE")

				//CHAR TYPE
				|| dataType.startsWith("CHAR")
				|| dataType.startsWith("CHARACTER")
				|| dataType.startsWith("NCHAR")

				//BLOB TYPE
				|| dataType.equals("BLOB")
				|| dataType.equals("TINYBLOB")
				|| dataType.equals("MEDIUMBLOB")
				|| dataType.equals("LONGBLOB")
				|| dataType.equals("IMAGE")
				|| dataType.equals("OID")

				//CLOG TYPE
				|| dataType.equals("CLOB")
				|| dataType.equals("TINYTEXT")
				|| dataType.equals("TEXT")
				|| dataType.equals("MEDIUMTEXT")
				|| dataType.equals("NTEXT")
				|| dataType.equals("NCLOB")

				//UUID TYPE
				|| dataType.equals("UUID")

				//ARRAY TYPE
				|| dataType.equals("ARRAY")


				//GEOMETRY TYPE
				|| dataType.equals("GEOMETRY")

				) {
			return true;
		}
		return false;
	}

	public static boolean isNumericType(String dataType) {
		if(isIntegerType(dataType)) {
			return true;
		} else if(isDoubleType(dataType)) {
			return true;
		}
		return false;
	}
	
	public static boolean isIntegerType(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.startsWith("BIT")
				|| dataType.startsWith("IDENTITY")
				|| dataType.startsWith("LONG")
				|| dataType.startsWith("INT")
				|| dataType.startsWith("INTEGER")
				|| dataType.startsWith("MEDIUMINT")
				|| dataType.startsWith("INT4")
				|| dataType.startsWith("SIGNED")
				
				//TINYINT TYPE
				|| dataType.startsWith("TINYINT")
				
				//SMALLINT TYPE
				|| dataType.startsWith("SMALLINT")
				|| dataType.startsWith("INT2")
				|| dataType.startsWith("YEAR")
				
				//BIGINT TYPE
				|| dataType.startsWith("BIGINT")
				|| dataType.startsWith("INT8")
				
				// PANDAS
				|| dataType.contains("DTYPE('INT64')")
				) {
			return true;
		}

		return false;
	}
	
	public static boolean isDoubleType(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.startsWith("NUMBER")
				|| dataType.startsWith("MONEY")
				|| dataType.startsWith("SMALLMONEY")
				|| dataType.startsWith("FLOAT")

				//DECIMAL TYPE
				|| dataType.startsWith("DECIMAL")
				|| dataType.startsWith("NUMBER")
				|| dataType.startsWith("DEC")
				|| dataType.startsWith("NUMERIC")

				//DOUBLE TYPE
				|| dataType.startsWith("DOUBLE")
				|| dataType.startsWith("PRECISION")
				|| dataType.startsWith("FLOAT")
				|| dataType.startsWith("FLOAT8")

				//REAL TYPE
				|| dataType.startsWith("REAL")
				|| dataType.startsWith("FLOAT4")

				// PANDAS
				|| dataType.contains("DTYPE('FLOAT64')")
				
				) {
			return true;
		}

		return false;
	}

	public static boolean isStringType(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.equals("STRING")
				//VARCHAR TYPE
				|| dataType.startsWith("VARCHAR")
				|| dataType.startsWith("TEXT")
				|| dataType.startsWith("LONGVARCHAR")
				|| dataType.startsWith("VARCHAR2")
				|| dataType.startsWith("NVARCHAR")
				|| dataType.startsWith("NVARCHAR2")
				|| dataType.startsWith("VARCHAR_CASESENSITIVE")

				//VARCHAR_IGNORECASE TYPE
				|| dataType.startsWith("VARCHAR_IGNORECASE")

				//CHAR TYPE
				|| dataType.startsWith("CHAR")
				|| dataType.startsWith("CHARACTER")
				|| dataType.startsWith("NCHAR")
				
				//R TYPE
				|| dataType.startsWith("FACTOR")

				// PANDAS
				|| dataType.contains("DTYPE('O')")

				) {
			return true;
		}

		return false;
	}

	public static boolean isDateType(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.startsWith("DATE")) {
			return true;
		}

		return false;
	}
	
	public static boolean isTimeStamp(String dataType) {
		dataType = dataType.toUpperCase().trim();		
		if(dataType.startsWith("TIMESTAMP")
				|| dataType.startsWith("DATETIME")
				) {
			return true;
		}
		
		return false;
	}
	
	//return the translation from sql types to h2 types
	private static Map<String, String> getH2TypeConversionMap() {
		Map<String, String> conversionMap = new HashMap<>();
		
		conversionMap.put("MONEY", "DECIMAL");
		conversionMap.put("SMALLMONEY", "DECIMAL");
		conversionMap.put("TEXT", "VARCHAR(800)");
		conversionMap.put("STRING", "VARCHAR(800)");
		conversionMap.put("NUMBER", "DOUBLE");
		
		return conversionMap;
	}

	/**
	 * Take the file location and return the original file name
	 * Based on upload flow, files that go through FileUploader.java class get appended with the date of upload
	 * in the format of "_yyyy_MM_dd_HH_mm_ss_SSSS" (length of 25) 
	 * Thus, we see if the file has the date appended and remove it if we find it
	 * @param FILE_LOCATION						The location of the file
	 * @param EXTENSION							The file extension
	 * @return									The original file name
	 */
	public static String getOriginalFileName(final String FILE_LOCATION) {
		// The FileUploader appends the time as "_yyyy_MM_dd_HH_mm_ss_SSSS"
		// onto the original fileName in order to ensure that it is unique
		// since we are using the fileName to be the table name, let us try and remove this
		String ext = "." + FilenameUtils.getExtension(FILE_LOCATION);
		String fileName = FilenameUtils.getName(FILE_LOCATION).replace(ext, "");
		// 24 is the length of the date being added
		if(fileName.length() > 28) {
			String fileEnd = fileName.substring(fileName.length()-24);
			try {
				new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").parse(fileEnd);
				// if the parsing was successful, we remove it from the fileName
				fileName = fileName.substring(0, fileName.length()-24);
			} catch (ParseException e) {
				// the end was not the added date, so do nothing
			}
		}
		return fileName + ext;
	}

	/**
	 * Loads an engine - sets the core properties, loads base data engine and ontology file.
	 * @param 	Filename.
	 * @param 	List of properties.

	 * @return 	Loaded engine. */
	public static IEngine loadEngine(String fileName, Properties prop) {
		IEngine engine = null;
		try {
			String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
			String engineId = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);

			if(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId)) {
				LOGGER.debug("DB " + engineId + " is already loaded...");
				// engines are by default loaded so that we can keep track on the front end of engine/all call
				// so even though it is added here there is a good possibility it is not loaded so check to see this
				if(DIHelper.getInstance().getLocalProp(engineId) instanceof IEngine) {
					return (IEngine) DIHelper.getInstance().getLocalProp(engineId);
				}
			}

			// we store the smss location in DIHelper 
			DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.STORE, fileName);
			// we also store the OWL location
			if(prop.containsKey(Constants.OWL)) {
				DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
			}
			
			// create and open the class
			engine = (IEngine)Class.forName(engineClass).newInstance();
			engine.setEngineId(engineId);
			engine.openDB(fileName);

			// set the engine in DIHelper
			DIHelper.getInstance().setLocalProperty(engineId, engine);

			// Append the engine name to engines if not already present
			if(!(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId))) {
				engines = engines + ";" + engineId;
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			}

			boolean isLocal = engineId.equals(Constants.LOCAL_MASTER_DB_NAME);
			boolean isSecurity = engineId.equals(Constants.SECURITY_DB);
			boolean isThemes = engineId.equals(Constants.THEMING_DB);
			if(!isLocal && !isSecurity && !isThemes) {
				// sync up the engine metadata now
				synchronizeEngineMetadata(engineId);
				SecurityUpdateUtils.addApp(engineId);
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return engine;
	}

	public static void synchronizeEngineMetadata(String engineId) {
		/*
		 * This determines when it is necessary to add an engine into the local master database
		 * 
		 * Logical Flow
		 * 
		 * 1) Get timestamp of the engine within local master -> call this time_local
		 * 2) Get timestamp of last engine update, stored on engine's OWL file -> call this time_engine
		 * 3) If time_local is equal to time_engine, local master is up to date
		 * 4) In all other conditions, add the engine to the local master
		 * 		-> all other conditions means if either time_local or time_engine are null or
		 * 			if they do not equal
		 * 
		 * Note: that after the local master has the engine added, the timestamp in the local master
		 * 			is set to be equal to that in the engine's OWL file (if the engine's OWL file time
		 * 			stamp is null, it is set to the time when this routine started running)
		 */

		// grab the local master engine
		IEngine localMaster = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
		if(localMaster == null) {
			LOGGER.info(">>>>>>>> Unable to find local master database in DIHelper.");
			return;
		}

		// generate the appropriate query to execute on the local master engine to get the time stamp
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(engineId + "_" + Constants.STORE);

		// this has all the details
		// the engine file is primarily the SMSS that is going to be utilized for the purposes of retrieving all the data
		Properties prop = Utility.loadProperties(smssFile);

		String rawType = prop.get(Constants.ENGINE_TYPE).toString();
		if(rawType.contains("AppEngine")) {
			// this engine has no data! it is just a collection of insights
			// nothing to synchronize into local master
			return;
		}
		
		// TODO: NEED TO STILL BUILD THIS OUT!
		if(rawType.contains("RemoteSemossEngine")) {
			return;
		}
		
		AddToMasterDB adder = new AddToMasterDB();

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date rdbmsDate = adder.getEngineDate(engineId);
		File owlFile = SmssUtilities.getOwlFile(prop);
		String engineDbTime = df.format(new Date(owlFile.lastModified()));

		// 4) perform the necessary additions if the time stamps do not equal
		// this is broken out into 2 separate parts
		// 4.1) the local master doesn't have a time stamp which means the engine is not present
		//		-> i.e. we do not need to remove the engine and re-add it
		// 4.2) the time is present and we need to remove anything relating the engine that was in the engine and then re-add it
		String engineRdbmsDbTime = "Dummy";
		if(rdbmsDate != null) {
			engineRdbmsDbTime = df.format(rdbmsDate);
		}
		
		if(rdbmsDate == null) {
			// logic to register the engine into the local master
			adder.registerEngineLocal(prop);
			adder.commit(localMaster);
		} else if(!engineRdbmsDbTime.equalsIgnoreCase(engineDbTime)) { 
			// if it has a time stamp, it means it was previously in local master
			// logic to delete an engine from the local master
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
			// logic to add the engine into the local master
			adder.registerEngineLocal(prop);
			adder.commit(localMaster);
		}
	}
	
	/**
	 * Splits up a URI into tokens based on "/" character and uses logic to return the instance name.
	 * @param String		URI to be split into tokens.

	 * @return String		Instance name. */
	public static String getInstanceName(String uri, IEngine.ENGINE_TYPE type) {
		StringTokenizer tokens = new StringTokenizer(uri + "", "/");
		int totalTok = tokens.countTokens();
		String instanceName = null;

		String secondLastToken = null;

		for (int tokIndex = 0; tokIndex <= totalTok && tokens.hasMoreElements(); tokIndex++) {
			if (tokIndex + 2 == totalTok) {
				secondLastToken = tokens.nextToken();
			} else if (tokIndex + 1 == totalTok) {
				instanceName = tokens.nextToken();
			} else {
				tokens.nextToken();
			}
		}

		if(type == IEngine.ENGINE_TYPE.RDBMS || type == IEngine.ENGINE_TYPE.R)
			instanceName = "Table_" + instanceName + "Column_" + secondLastToken;  

		return instanceName;
	}

	public static String getRandomString(int len)
	{
		String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";

		String retString = "a";
		for(int i = 0;i< len;i++)
		{
			double num = Math.random()*alpha.length();
			retString = retString + alpha.charAt(new Double(num).intValue());
		}

		return retString;
	}
	
	/**
	 * 
	 * @param engineId - engine to get
	 * @return
	 * 
	 * Use this method to get the engine when the engine hasn't been loaded
	 */
	@Deprecated
	public static IEngine getEngine(String engineId) {
		return getEngine(engineId, true);
	}
	
	/**
	 * 
	 * @param engineId - engine to get
	 * @return
	 * 
	 * Use this method to get the engine when the engine hasn't been loaded
	 */
	@Deprecated
	public static IEngine getEngine(String engineId, boolean pullIfNeeded) {
		IEngine engine = null;

		// If the engine has already been loaded, then return it
		// Don't acquire the lock here, because that would slow things down
		if (DIHelper.getInstance().getLocalProp(engineId) != null) {
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineId);

			// TODO >>>timb: why is this here?
			engineLocks.remove(engineId);

		// Else, need to load the engine
		} else {

			// Acquire the lock on the engine,
			// don't want several calls to try and load the engine at the same
			// time
			ReentrantLock lock = getEngineLock(engineId);
			lock.lock();

			// Need to do a double check here,
			// so if a different thread was waiting for the engine to load,
			// it doesn't go through this process again
			if (DIHelper.getInstance().getLocalProp(engineId) != null) {
				return (IEngine) DIHelper.getInstance().getLocalProp(engineId);
			}

			// If in a clustered environment, then pull the app first
			// TODO >>>timb: need to pull sec and lmd each time. They also need
			// correct jdbcs...
			try {
				if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
					try {
						AZClient.getInstance().pullApp(engineId);
					} catch (IOException | InterruptedException e) {
						e.printStackTrace();
						return null;
					}
				}

				// Now that the app has been pulled, grab the smss file
				String smssFile = (String) DIHelper.getInstance().getCoreProp()
						.getProperty(engineId + "_" + Constants.STORE);

				// Start up the engine using the details in the smss
				if (smssFile != null) {

					// Actual process to load
					// TODO >>>timb: clean this up while we are at it
					FileInputStream fis = null;
					try {
						Properties daProp = new Properties();
						fis = new FileInputStream(smssFile);
						daProp.load(fis);
						engine = Utility.loadEngine(smssFile, daProp);
						System.out.println("Loaded the engine.. !!!!! " + engineId);
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						if (fis != null) {
							try {
								fis.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				} else {
					System.out.println("There is no SMSS File for the engine " + engineId + "...");
				}
				
				// TODO >>>timb: Centralize this ZK env check stuff and use is cluster variable
				// TODO >>>timb: remove node exists error or catch it 
				// TODO >>>cluster: tag
				// Start with because the insights RDBMS has the id security_InsightsRDBMS
				if (!(engineId.startsWith("security") || engineId.startsWith("LocalMasterDatabase") || engineId.startsWith("form_builder_engine") || engineId.startsWith("themes"))) {
					Map<String, String> envMap = System.getenv();
					if(envMap.containsKey(ZKClient.ZK_SERVER) || envMap.containsKey(ZKClient.ZK_SERVER.toUpperCase())) {
						if(ClusterUtil.LOAD_ENGINES_LOCALLY) {
							
							// Only publish if actually loading on this box
							// TODO >>>timb: this logic only works insofar as we are assuming a user-based docker layer in addition to the app containers
							String host = "unknown";
							
							if(envMap.containsKey(ZKClient.HOST))
								host = envMap.get(ZKClient.HOST);
							
							if(envMap.containsKey(ZKClient.HOST.toUpperCase()))
								host = envMap.get(ZKClient.HOST.toUpperCase());
							
							// we are in business
							ZKClient client = ZKClient.getInstance();
							client.publishDB(engineId + "@" + host);
						}
					}
				}
			} finally {
				
				// Make sure to unlock now
				lock.unlock();
			}
		}

		return engine;
	}
	
	private static ReentrantLock getEngineLock(String engineName) {
		engineLocks.putIfAbsent(engineName, new ReentrantLock());
		return engineLocks.get(engineName);
	}
	
	public static HashMap<String, Object> getPKQLInputVar(String param, String reactor){
		HashMap<String, Object> inputMap = new HashMap<String, Object>();
		Object restrictions = new Object();
		
		switch(param){
		case "COL_DEF" : inputMap.put("dataType", "column");						 
						 inputMap.put("restrictions", restrictions);
						 inputMap.put("source", "");
						 switch(reactor){//COL_DEF specifies different var for some reactors - for COL_ADD its new column name, for COL_SPLIT, its existinmg column name
							case "COL_ADD": inputMap.put("label", "New Column Name");
											inputMap.put("varName", "c:newCol");
											inputMap.put("type", "freetext");
											inputMap.put("values", "");break;
											
							case "COL_SPLIT": inputMap.put("label", "Column to be split");
											  inputMap.put("varName", "c:col1");											  
											  inputMap.put("type", "dropdown");
											  inputMap.put("values", "");break;
											  
							case "UNFILTER_DATA": inputMap.put("label", "Column to be unfiltered");
							  					  inputMap.put("varName", "c:col1");											  
							  					  inputMap.put("type", "dropdown");
							  					  inputMap.put("values", "");break;

							}break;
							
		case "EXPR_TERM" : inputMap.put("label", "New Column Value");
						   inputMap.put("varName", "expression");
						   inputMap.put("dataType", "expression");
						   inputMap.put("type", "freetext");
						   inputMap.put("restrictions", restrictions);
						   inputMap.put("source", "");break;
						   
		case "WORD_OR_NUM" : inputMap.put("dataType", "text");		  					 
		  					 inputMap.put("restrictions", restrictions);
		  					 inputMap.put("source", "");
		  					 switch(reactor){
								case "COL_SPLIT": inputMap.put("label", "Delimiter");
												  inputMap.put("varName", "delimiter");
												  inputMap.put("type", "freetext");
												  break;
								}break;
								
		case "FILTERS" : inputMap.put("label", "Column with unfiltered data");
		   				 inputMap.put("varName", "c:col1=[instances]");
		   				 inputMap.put("dataType", "column");
		   				 inputMap.put("type", "filterDropdown");
		   				 inputMap.put("restrictions", restrictions);
		   				 inputMap.put("source", "");
		   				 inputMap.put("values", "");break;
								
		default: break;
		}
		return inputMap;
	}

	public static String findOpenPort()
	{
		// start with 7677 and see if you can find any
		System.out.println("Finding an open port.. ");
		boolean found = false;
		int port = 5355;int count = 0;
		//String server = "10.13.229.203";
		String server = "127.0.0.1";
		for(;!found && count < 10000;port++, count++)
		{
			System.out.print("Trying.. " + port);
			try
			{
				ServerSocket s = new ServerSocket(port) ;//"10.13.229.203", port);
				//s.connect(new InetSocketAddress(server, port), 5000);//"localhost", port);
				//s.accept();
				found = true;
				s.close();
				System.out.println("  Success !!!!");
				//no error, found an open port, we can stop
				break;
			}catch (Exception ex)
			{
				// do nothing
				//ex.printStackTrace();
				System.out.println("  Fail");
				//System.exit(0);
				found = false;
				//ex.printStackTrace();
			}finally
			{
			}
		}
		
		//if we found a port, return that port
		if(found) return port+"";
				
		port--;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String portStr = null;
		if(!found) {
			System.out.println("Unable to find an open port. Please provide a port.");
			 try {
				portStr = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Using port: " + portStr);
		} else {
			portStr = port+"";
		}
		
		return portStr;
	}
	
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
		return Utility.writeResultToFile(fileLocation, it, typesMap, ",");
	}
	
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap, String seperator) {
		long start = System.currentTimeMillis();
		
		// make sure file is empty so we are only inserting the new values
		File f = new File(fileLocation);
		if(f.exists()) {
			System.out.println("File currently exists.. deleting file");
			f.delete();
		}
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		BufferedWriter bufferedWriter = null;
		
		try {
			fos = new FileOutputStream(f);
			osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
	        bufferedWriter = new BufferedWriter(osw);

	        // store some variables and just reset
	        // should be faster than creating new ones each time
	        int i = 0;
	        int size = 0;
	        StringBuilder builder = null;
	        // create typesArr as an array for faster searching
	        String[] headers = null;
	        SemossDataType[] typesArr = null;
	        
	        // we need to iterate and write the headers during the first time
			if(it.hasNext()) {
				IHeadersDataRow row = it.next();
				
				// generate the header row
				// and define constants used throughout like size, and types
				i = 0;
				headers = row.getHeaders();
				size = headers.length;
				typesArr = new SemossDataType[size];
				builder = new StringBuilder();
				for(; i < size; i++) {
					builder.append("\"").append(headers[i]).append("\"");
					if( (i+1) != size) {
						builder.append(seperator);
					}
					typesArr[i] = typesMap.get(headers[i]);
					if(typesArr[i] == null) {
						typesArr[i] = SemossDataType.STRING;
					}
				}
				// write the header to the file
				bufferedWriter.write(builder.append("\n").toString());
				
				// generate the data row
				Object[] dataRow = row.getValues();
				builder = new StringBuilder();
				i = 0;
				for(; i < size; i ++) {
					if(typesArr[i] == SemossDataType.STRING) {
						builder.append("\"").append(dataRow[i]).append("\"");
					} else {
						builder.append(dataRow[i]);
					}
					if( (i+1) != size) {
						builder.append(seperator);
					}
				}
				// write row to file
				bufferedWriter.write(builder.append("\n").toString());
			}
			
			// now loop through all the data
			while(it.hasNext()) {
				IHeadersDataRow row = it.next();
				// generate the data row
				Object[] dataRow = row.getValues();
				builder = new StringBuilder();
				i = 0;
				for(; i < size; i ++) {
					if(typesArr[i] == SemossDataType.STRING) {
						builder.append("\"").append(dataRow[i]).append("\"");
					} else {
						builder.append(dataRow[i]);
					}
					if( (i+1) != size) {
						builder.append(seperator);
					}
				}
				// write row to file
				bufferedWriter.write(builder.append("\n").toString());
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(osw != null) {
					osw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fos != null) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Time to output file = " + (end-start) + " ms");
		
		return f;
	}

	
	public static String encodeURIComponent(String s) {
		try {
			s = URLEncoder.encode(s, "UTF-8")
					.replaceAll("\\+", "%20")
					.replaceAll("\\%21","!")
					.replaceAll("\\%27","'")
					.replaceAll("\\%28","(")
					.replaceAll("\\%29",")")
					.replaceAll("\\%7E","~");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return s;
	}
	
	public static String decodeURIComponent(String s) {
		try {
			String newS = s.replaceAll("\\%20", "+")
					.replaceAll("!","%21")
					.replaceAll("'","%27")
					.replaceAll("\\(","%28")
					.replaceAll("\\)","%29")
					.replaceAll("~","%7E");
			s = URLDecoder.decode(newS, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return s;
	}
	
	/**
	 * Loads the properties from a specifed properties file.
	 * 
	 * @param fileName
	 *            String of the name of the properties file to be loaded.
	 * @return Properties The properties imported from the prop file.
	 */
	public static Properties loadProperties(String fileName) {
		Properties retProp = new Properties();
		FileInputStream fis = null;
		if (fileName != null) {
			try {
				fis = new FileInputStream(fileName);
				retProp.load(fis);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		return retProp;
	}
	public static void copyURLtoFile(String urlString, String filePath) {
		try {
			URL url = new URL(urlString);
			URLConnection conn = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;
			// write file
			PrintWriter out = new PrintWriter(filePath);
			while ((inputLine = in.readLine()) != null) {
				out.write(inputLine + System.getProperty("line.separator"));
			}
			out.close();
			in.close();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	/**
//	 * Update old insights... hope we get rid of this soon
//	 * @param insightRDBMS
//	 * @param engineName
//	 */
//	public static void updateOldInsights(IEngine engine) {
//		String query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'QUESTION_ID'";
//
//		// need to get the insight
//		IEngine insightRDBMS = engine.getInsightDatabase();
//		if(insightRDBMS == null) {
//			LOGGER.info(engine.getEngineName() + " does not have an insight rdbms");
//			return;
//		}
//		
//		Map<String, Object> mapRet = (Map<String, Object>) insightRDBMS.execQuery(query);
//		Statement stat = (Statement) mapRet.get(RDBMSNativeEngine.STATEMENT_OBJECT);
//		ResultSet rs = (ResultSet) mapRet.get(RDBMSNativeEngine.RESULTSET_OBJECT);
//		try {
//			if (rs.next()) {
////				insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_LAYOUT = 'Graph' WHERE p.QUESTION_DATA_MAKER = 'GraphDataModel'");
//				insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_DATA_MAKER = REPLACE(QUESTION_DATA_MAKER, 'BTreeDataFrame', 'TinkerFrame')");
//				insightRDBMS.insertData("UPDATE QUESTION_ID p SET QUESTION_MAKEUP = REPLACE(QUESTION_MAKEUP, 'SELECT @Concept-Concept:Concept@, ''http://www.w3.org/1999/02/22-rdf-syntax-ns#type'', ''http://semoss.org/ontologies/Concept''', 'SELECT @Concept-Concept:Concept@') WHERE p.QUESTION_DATA_MAKER = 'TinkerFrame'");
//				insightRDBMS.insertData("UPDATE QUESTION_ID SET QUESTION_DATA_MAKER='TinkerFrame' WHERE QUESTION_NAME='Explore a concept from the database' OR QUESTION_NAME='Explore an instance of a selected node type'"); 
//				
//				// also update the base explore instance query
//				Utility.updateExploreInstanceInsight(engine);
//			} else {
//				LOGGER.error("COULD NOT FIND INSIGHTS QUESTION_ID TABLE FOR ENGINE = " + engine.getEngineName());
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			if (rs != null) {
//				try {
//					rs.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//			if(stat != null) {
//				try {
//					stat.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}
//	
//	// i hope this doesn't need to stay here for long
//	// only because we have a dumb way of passing insights as old question file
//	// instead of keeping what is in the insights rdbms
//	public static void updateExploreInstanceInsight(IEngine engine) {
//		// ignore local master db
//		// this is important since DataMakerComponent will try to load the engine
//		// but the engine is already in a lock since this is called when the engine is called
//		// this will be cleaned up once we get rid of DMC
//		String engineName = engine.getEngineName();
//		if(engineName.equals(Constants.LOCAL_MASTER_DB_NAME)) {
//			return;
//		}
//		
//		String insightId = null;
//		
//		// need to get the insight
//		IEngine insightRDBMS = engine.getInsightDatabase();
//		if(insightRDBMS == null) {
//			LOGGER.info(engineName + " does not have an insight rdbms");
//			return;
//		}
//		
//		// to delete from solr, we need to get the insight id
//		Map<String, Object> queryMap = (Map<String, Object>) insightRDBMS.execQuery("SELECT ID FROM QUESTION_ID p WHERE p.QUESTION_NAME = 'Explore an instance of a selected node type' OR p.QUESTION_NAME = 'Explore a concept from the database'");
//		try {
//			ResultSet rs = (ResultSet) queryMap.get(RDBMSNativeEngine.RESULTSET_OBJECT);
//			if(rs != null) {
//				while(rs.next()) {
//					insightId = rs.getObject(1) + "";
//				}
//				
//				// close the streams
//				Statement stmt = (Statement) queryMap.get(RDBMSNativeEngine.STATEMENT_OBJECT);
//				rs.close();
//				stmt.close();
//			}
//			
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		
//		if(insightId == null) {
//			LOGGER.info(engineName + " does not have explore an instance query in database to update");
//			return;
//		}
//		
//		// now we need to modify the insight DMC list to be the new values
//		QuestionAdministrator questionAdmin = new QuestionAdministrator(engine);
//		
//		String insightName = "Explore an instance of a selected node type";
//		String perspective = "Generic-Perspective";
//		String layout = "Graph";
//		String order = "1";
//		String dmName = "TinkerFrame";
//		boolean isDbQuery = false;
//		Map<String, String> dataTableAlign = new HashMap<String, String>();
//		List<SEMOSSParam> params = new Vector<SEMOSSParam>();
//		String uiOptions = "";
//		
//		// create an empty comp and add the pkqls 
//		Vector<DataMakerComponent> dmcList = new Vector<DataMakerComponent>();
//		DataMakerComponent emptyComp = new DataMakerComponent(Constants.LOCAL_MASTER_DB_NAME, Constants.EMPTY);
//		dmcList.add(emptyComp);
//
//		// add the data.model pkql
//		PKQLTransformation trans = new PKQLTransformation();
//		Map<String, Object> prop = new HashMap<String, Object>();
//		String pkqlCmd = "data.model(<json>{\"jsonView\":[{\"title\":\"Select Parameters:\",\"Description\":\"Explore instances of selected concept\",\"pkqlCommand\":\""
//				+ "data.frame('graph');data.import(api:<engine>.query([c:<concept>],(c:<concept>=[<instance>])));panel[0].viz(Graph,[]);\",\"input\": {\"concept\": "
//				+ "{\"name\": \"concept\",\"type\":\"dropdown\",\"required\": true,\"label\": \"Concept\",\"optionsGetter\": {\"command\": \"database.concepts(<engine>);"
//				+ "\",\"dependInput\": []},\"options\": [],\"value\": \"\"},\"instance\": {\"name\": \"instance\",\"type\": \"checkBox\",\"required\": true,\"label\": "
//				+ "\"Instance\",\"optionsGetter\": {\"command\": \"data.query(api:<engine>.query([c:<concept>],{'limit':50, 'offset':0, 'getCount': 'false'}));\","
//				+ "\"dependInput\": [\"concept\"]},\"options\": [],\"value\": \"\"},\"execute\": {\"name\": \"execute\",\"type\": \"buttonGroup\",\"required\": true,"
//				+ "\"position\": \"bottom\",\"label\": \"\",\"optionsGetter\": [],\"options\": [\"Execute\"],\"value\": \"\",\"attribute\": {\"buttonGroupAttr\": "
//				+ "\"style='display:block'\"}}}}]}</json>);";		
//		pkqlCmd = pkqlCmd.replace("<engine>", engine.getEngineName());
//		prop.put(PKQLTransformation.EXPRESSION, pkqlCmd);
//		trans.setProperties(prop);
//		emptyComp.addPostTrans(trans);
//		
//		questionAdmin.modifyQuestion(insightId, insightName, perspective, dmcList, layout, order, dmName, isDbQuery, dataTableAlign, params, uiOptions);
//		
//		Map<String, Object> solrMap = new HashMap<String, Object>();
//		// in case we modified the explore instance name since some legacy insights have difference in naming between rdf and rdbms
//		solrMap.put(SolrIndexEngine.INDEX_NAME, insightName);
//		solrMap.put(SolrIndexEngine.STORAGE_NAME, insightName);
//		try {
//			SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + insightId, solrMap);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
//				| IOException e) {
//			e.printStackTrace();
//		}
//	}

}
