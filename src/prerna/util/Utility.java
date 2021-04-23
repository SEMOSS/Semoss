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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.net.URLClassLoader;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Workbook;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.owasp.encoder.Encode;
import org.owasp.esapi.ESAPI;

import com.google.common.base.Strings;
import com.google.gson.GsonBuilder;
import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.CloudClient;
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
import prerna.om.AppAvailabilityStore;
import prerna.om.IStringExportProcessor;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.task.ITask;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.git.GitAssetUtils;

/**
 * The Utility class contains a variety of miscellaneous functions implemented
 * extensively throughout SEMOSS. Some of these functionalities include getting
 * concept names, printing messages, loading engines, and writing Excel
 * workbooks.
 */
public class Utility {

	public static int id = 0;
	private static final Logger logger = LogManager.getLogger(prerna.util.Utility.class);

	private static final String SPECIFIED_PATTERN = "[@]{1}\\w+[-]*[\\w/.:]+[@]";
	private static Map <String, String> engineIdMap = new HashMap<String, String>();

	/**
	 * Matches the given query against a specified pattern. While the next substring
	 * of the query matches a part of the pattern, set substring as the key with
	 * EMPTY constants (@@) as the value
	 * 
	 * @param Query.
	 * 
	 * @return Hashtable of queries to be replaced
	 */
	public static Hashtable getParams(String query) {
		Hashtable paramHash = new Hashtable();
		Pattern pattern = Pattern.compile(SPECIFIED_PATTERN);

		Matcher matcher = pattern.matcher(query);
		while (matcher.find()) {
			String data = matcher.group();
			data = data.substring(1, data.length() - 1);
			logger.debug(data);
			// put something to strip the @
			paramHash.put(data, Constants.EMPTY);
		}

		return paramHash;
	}

	/**
	 * Matches the given query against a specified pattern. While the next substring
	 * of the query matches a part of the pattern, set substring as the key with
	 * EMPTY constants (@@) as the value
	 * 
	 * @param Query.
	 * 
	 * @return Hashtable of queries to be replaced
	 */
	public static Hashtable getParamTypeHash(String query) {
		Hashtable paramHash = new Hashtable();
		Pattern pattern = Pattern.compile(SPECIFIED_PATTERN);

		Matcher matcher = pattern.matcher(query);
		while (matcher.find()) {
			String data = matcher.group();
			data = data.substring(1, data.length() - 1);
			String paramName = data.substring(0, data.indexOf("-"));
			String paramValue = data.substring(data.indexOf("-") + 1);

			logger.debug(data);
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
		s = s.replace("&gt;", ">");
		s = s.replace("&lt;", "<");
		s = s.replace("&#61;", "=");
		s = s.replace("&#33;", "!");
		return s;
	}

	/**
	 * Matches the given query against a specified pattern. While the next substring
	 * of the query matches a part of the pattern, set substring as the key with
	 * EMPTY constants (@@) as the value
	 * 
	 * @param Query.
	 * 
	 * @return Hashtable of queries to be replaced
	 */
	public static String normalizeParam(String query) {
		Map<String, List<Object>> paramHash = new Hashtable<>();
		Pattern pattern = Pattern.compile(SPECIFIED_PATTERN);

		Matcher matcher = pattern.matcher(query);
		while (matcher.find()) {
			String data = matcher.group();
			data = data.substring(1, data.length() - 1);
			if (data.contains("-")) {
				String paramName = data.substring(0, data.indexOf('-'));

				logger.debug(data);
				// put something to strip the @
				List<Object> retList = new ArrayList<>();
				retList.add("@" + paramName + "@");
				paramHash.put(data, retList);
			}
		}

		return fillParam(query, paramHash);
	}

	/**
	 * Gets the param hash and replaces certain queries
	 * 
	 * @param Original  query
	 * @param Hashtable of format [String to be replaced] [Replacement]
	 * 
	 * @return If applicable, returns the replaced query
	 */
	public static String fillParam(String query, Map<String, List<Object>> paramHash) {
		// NOTE: this process always assumes only one parameter is selected
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		logger.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key).get(0) + "";
			logger.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if (!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}

		return query;
	}

	/**
	 * Gets the param hash and replaces certain queries
	 * 
	 * @param Original  query
	 * @param Hashtable of format [String to be replaced] [Replacement]
	 * 
	 * @return If applicable, returns the replaced query
	 */
	public static String fillParam2(String query, Map<String, String> paramHash) {
		// NOTE: this process always assumes only one parameter is selected
		// Hashtable is of pattern <String to be replaced> <replacement>
		// key will be surrounded with @ just to be in sync
		logger.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key);
			logger.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if (!value.equalsIgnoreCase(Constants.EMPTY))
				query = query.replace("@" + key + "@", value);
		}

		return query;
	}

	/**
	 * Splits up a URI into tokens based on "/" character and uses logic to return
	 * the instance name.
	 * 
	 * @param String URI to be split into tokens.
	 * 
	 * @return String Instance name.
	 */
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
	 * Splits up a URI into tokens based on "/" character and uses logic to return
	 * the node instance name for a display name of a property where the node's
	 * display name is prior to the property display name.
	 * 
	 * @param String URI to be split into tokens.
	 * @return String Instance name.
	 */
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
	 * Splits up a URI into tokens based on "/" character and uses logic to return
	 * the primary key.
	 * 
	 * @param String URI to be split into tokens.
	 *               (BASE_URI/Concept/PRIMARY_KEY/INSTANCE_NAME
	 * 
	 * @return String Primary Key
	 */
	public static String getPrimaryKeyFromURI(String uri) {
		String[] elements = uri.split("/");
		if (elements.length >= 2) {
			return elements[elements.length - 2];
		} else {
			return uri;
		}
	}

	public static String getFQNodeName(IEngine engine, String URI) {
		if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)) {
			return getInstanceName(URI) + "__" + getPrimaryKeyFromURI(URI);
		} else {
			return getInstanceName(URI);
		}
	}

	/**
	 * Splits up a URI into tokens based on "/" character and uses logic to return
	 * the base URI
	 * 
	 * @param String URI to be split into tokens.
	 * @return String Base URI
	 */
	public static String getBaseURI(String uri) {
		int indexOf = uri.lastIndexOf('/');
		String baseURI = uri.substring(0, indexOf);

		indexOf = baseURI.lastIndexOf('/');
		baseURI = baseURI.substring(0, indexOf);

		indexOf = baseURI.lastIndexOf('/');
		baseURI = baseURI.substring(0, indexOf);

		return baseURI;
	}

	/**
	 * Go through all URIs in list, splits them into tokens based on "/", and uses
	 * logic to return the instance names.
	 * 
	 * @param Vector<String> List of URIs to be tokenized.
	 * 
	 * @return Hashtable with instance name as the keys mapped to the tokens as
	 *         values
	 */
	public static Hashtable<String, String> getInstanceNameViaQuery(Vector<String> uri) {
		if (uri.isEmpty()) {
			return new Hashtable<>();
		}

		Hashtable<String, String> retHash = new Hashtable<>();

		/*JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
		// loads all of the labels
		// http://www.w3.org/2000/01/rdf-schema#label
		String labelQuery = "";
		
		//fill all uri for binding string
		StringBuffer bindingStr = new StringBuffer("");
		for (int i = 0; i<uri.size();i++)
		{
			if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
				bindingStr = bindingStr.append("(<").append(uri.get(i)).append(">)");
			else
				bindingStr = bindingStr.append("<").append(uri.get(i)).append(">");
		}
		Hashtable paramHash = new Hashtable();
		paramHash.put("FILTER_VALUES",  bindingStr.toString());
		if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
		{			
			labelQuery = "SELECT DISTINCT ?Entity ?Label WHERE " +
					"{{?Entity <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
					"}" +"BINDINGS ?Entity {@FILTER_VALUES@}";
		}
		else if(engine.getEngineType() == IEngine.ENGINE_TYPE.JENA)
		{
			labelQuery = "SELECT DISTINCT ?Entity ?Label WHERE " +
					"{{VALUES ?Entity {@FILTER_VALUES@}"+
					"{?Entity <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
					"}";
		}
		labelQuery = Utility.fillParam(labelQuery, paramHash);
		
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(labelQuery);
		System.out.println(labelQuery);
		sjsw.executeQuery();
		sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement st = sjsw.next();
			String label = st.getVar("Label")+"";
			label = label.substring(1,label.length()-1);
			String uriValue = st.getRawVar("Entity")+ "";
			uri.removeElement(uriValue);
			retHash.put(label,  uriValue);
		}*/

		for (int i = 0; i < uri.size(); i++) {
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
	 * Executes a query on a specific engine, iterates through variables from the
	 * sesame wrapper, and uses logic to obtain the concept URI.
	 * 
	 * @param Specified engine.
	 * @param Subject   URI.
	 * 
	 * @return Concept URI.
	 */
	public static String getConceptType(IEngine engine, String subjectURI) {
		if (!subjectURI.startsWith("http://")) {
			return "";
		}

		String query = DIHelper.getInstance().getProperty(Constants.SUBJECT_TYPE_QUERY);
		Map<String, List<Object>> paramHash = new Hashtable<>();
		List<Object> values = new ArrayList<>();
		values.add(subjectURI);
		paramHash.put("ENTITY", values);
		query = Utility.fillParam(query, paramHash);

		ISelectWrapper sjw = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		sjw.setEngine(engine);
		sjw.setEngineType(engine.getEngineType());
		sjw.setQuery(query);
		sjw.executeQuery();*/
		String[] vars = sjw.getVariables();
		String returnType = null;
		while (sjw.hasNext()) {
			ISelectStatement stmt = sjw.next();
			String objURI = stmt.getRawVar(vars[0]) + "";
			if (!objURI.equals(DIHelper.getInstance().getProperty(Constants.SEMOSS_URI) + "/Concept")) {
				returnType = objURI;
			}

		}
		if (returnType == null) {
			returnType = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI) + "/Concept";
		}

		return returnType;
	}

	/**
	 * Splits up a URI into tokens based on "/" delimiter and uses logic to return
	 * the class name.
	 * 
	 * @param URI.
	 * 
	 * @return Name of class.
	 */
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
	 * 
	 * @return Next ID
	 */
	public static String getNextID() {
		id++;
		return Constants.BLANK_URL + "/" + id;
	}

	/**
	 * Gets the instance and class names for a specified URI and creates the
	 * qualified class name.
	 * 
	 * @param URI.
	 * 
	 * @return Qualified URI.
	 */
	public static String getQualifiedClassName(String uri) {
		// there are three patterns
		// one is the /
		// the other is the #
		// need to have a check upfront to see

		String instanceName = getInstanceName(uri);

		String className = getClassName(uri);
		String qualUri = "";
		if (uri.indexOf('/') >= 0) {
			instanceName = "/" + instanceName;
		}

		// remove this in the end
		if (className == null) {
			qualUri = uri.replace(instanceName, "");
		} else {
			qualUri = uri.replace(className + instanceName, className);
		}

		return qualUri;
	}

	/**
	 * Checks to see if a string contains a particular pattern. Used when adding
	 * relations.
	 * 
	 * @param Pattern
	 * @param String  to compare to the pattern
	 * 
	 * @return True if the next token is greater than or equal to zero.
	 */
	public static boolean checkPatternInString(String pattern, String string) {
		// ok.. before you think that this is so stupid why wont you use the regular
		// java.lang methods.. consider the fact that this could be a ; delimited
		// pattern
		boolean matched = false;
		StringTokenizer tokens = new StringTokenizer(pattern, ";");
		while (tokens.hasMoreTokens() && !matched) {
			matched = string.indexOf(tokens.nextToken()) >= 0;
		}

		return matched;
	}

	/**
	 * Used when selecting a repository.
	 * 
	 * @param Used  to retrieve successive elements.
	 * @param Size.
	 * 
	 * @return List of returned strings
	 */
	public static Vector<String> convertEnumToStringVector(Enumeration enums, int size) {
		Vector<String> retString = new Vector<>();
		while (enums.hasMoreElements()) {
			retString.add((String) enums.nextElement());
		}
		return retString;
	}

	/**
	 * Runs a check to see if calculations have already been performed.
	 * 
	 * @param Query (calculation) to be run on a specific engine.
	 * 
	 * @return True if calculations have been performed.
	 */
	public static boolean runCheck(String query) {
		boolean check = true;

		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		List<String> repos = list.getSelectedValuesList();

		ISelectWrapper selectWrapper = null;// WrapperManager.getInstance().getSWrapper(engine, queries.get(tabName));

		// SesameJenaSelectWrapper selectWrapper = null;
		for (String s : repos) {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(s);
			selectWrapper = WrapperManager.getInstance().getSWrapper(engine, query);

			/*selectWrapper = new SesameJenaSelectWrapper();
			selectWrapper.setEngine(engine);
			selectWrapper.setQuery(query);
			selectWrapper.executeQuery();*/
		}

		// if the wrapper is not empty, calculations have already been performed.
		if (selectWrapper != null && !selectWrapper.hasNext()) {
			check = false;
		}

		return check;
	}

	/**
	 * Displays error message.
	 * 
	 * @param Text to be displayed.
	 */
	public static void showError(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text, "Error", JOptionPane.ERROR_MESSAGE);
	}

	/**
	 * Displays confirmation message.
	 * 
	 * @param Text to be displayed.
	 */
	public static Integer showConfirm(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		return JOptionPane.showConfirmDialog(playPane, text);

	}

	/**
	 * Displays a message on the screen.
	 * 
	 * @param Text to be displayed.
	 */
	public static void showMessage(String text) {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, text);

	}

	/**
	 * Method round.
	 * 
	 * @param valueToRound          double
	 * @param numberOfDecimalPlaces int
	 * 
	 * @return double
	 */
	public static double round(double valueToRound, int numberOfDecimalPlaces) {
		BigDecimal bigD = new BigDecimal(valueToRound);
		return bigD.setScale(numberOfDecimalPlaces, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	/**
	 * Used to round a value to a specific number of decimal places.
	 * 
	 * @param Value  to round.
	 * @param Number of decimal places to round to.
	 * 
	 * @return Rounded value.
	 */
	public static String sciToDollar(double valueToRound) {
		double roundedValue = Math.round(valueToRound);
		DecimalFormat df = new DecimalFormat("#0");
		NumberFormat formatter = NumberFormat.getCurrencyInstance();
		df.format(roundedValue);

		return formatter.format(roundedValue);
	}

	// public static void main(String[] args) {
	// String date = "qweqweqw";
	// System.out.println(isStringDate(date));
	// }

	public static boolean isStringDate(String inDate) {
		List<SimpleDateFormat> dateFormatList = new ArrayList<>();

		dateFormatList.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'"));
		dateFormatList.add(new SimpleDateFormat("MM-dd-yyyy__HH:mm:ss_a"));

		for (SimpleDateFormat format : dateFormatList) {
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
		if (dataType.startsWith("FACTOR") || dataType.startsWith("ORDER")) {
			return true;
		}

		return false;
	}

	/**
	 * Changes a value within the SMSS file for a given key
	 * 
	 * @param smssPath       The path to the SMSS file
	 * @param keyToAlter     The key to alter
	 * @param valueToProvide The value to give the key
	 */
	public static void changePropMapFileValue(String smssPath, String keyToAlter, String valueToProvide) {
		changePropMapFileValue(smssPath, keyToAlter, valueToProvide, false);
	}

	public static void changePropMapFileValue(String smssPath, String keyToAlter, String valueToProvide,
			boolean contains) {
		FileOutputStream fileOut = null;
		File file = new File(smssPath);

		/*
		 * 1) Loop through the smss file and add each line as a list of strings
		 * 2) For each line, see if it starts with the key to alter
		 * 3) if yes, write out the key with the new value passed in
		 * 4) if no, just write out the line as is
		 * 
		 */
		List<String> content = new ArrayList<>();
		BufferedReader reader = null;
		FileReader fr = null;
		try {
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			// 1) add each line as a different string in list
			while ((line = reader.readLine()) != null) {
				content.add(line);
			}

			fileOut = new FileOutputStream(file);
			byte[] lineBreak = "\n".getBytes();
			// 2) iterate through each line if the smss file
			for (int i = 0; i < content.size(); i++) {
				// 3) if this line starts with the key to alter

				if (contains) {
					if (content.get(i).contains(keyToAlter)) {
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
					if (content.get(i).startsWith(keyToAlter + "\t") || content.get(i).startsWith(keyToAlter + " ")) {
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
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} finally {
			// close the readers
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}

			try {
				if (fileOut != null) {
					fileOut.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
		}
	}

	/**
	 * Adds a new key-value pair into the SMSS file
	 * 
	 * @param smssPath       The path of the smss file
	 * @param keyToAdd       The key to add into the smss file
	 * @param valueToProvide The value for the key to add to the smss file
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

		List<String> content = new ArrayList<>();
		BufferedReader reader = null;
		FileReader fr = null;
		try {
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			// 1) add each line as a different string in list
			while ((line = reader.readLine()) != null) {
				content.add(line);
			}

			fileOut = new FileOutputStream(file);
			for (int i = 0; i < content.size(); i++) {
				// 2) write out each line into the file
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());

				// 3) if the last line printed matches that in locInFile, then write the new
				// key-value pair after
				if (content.get(i).startsWith(locInFile + "\t") || content.get(i).startsWith(locInFile + " ")) {
					String newProp = keyToAdd + "\t" + valueToProvide;
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} finally {
			// close the readers
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}

			try {
				if (fileOut != null) {
					fileOut.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
		}
	}

	/**
	 * Cleans a string based on certain patterns
	 * 
	 * @param Original string
	 * @param If       true, replace forward slashes ("/") with dashes ("-")
	 * 
	 * @return Cleaned string
	 */
	public static String cleanString(String original, boolean replaceForwardSlash) {
		return cleanString(original, replaceForwardSlash, true, false);
	}

	/**
	 * Cleans a string based on certain patterns
	 * 
	 * @param Original      string
	 * @param If            true, replace forward slashes ("/") with dashes ("-")
	 * @param replaceForRDF If true, replace double quote with single quote and
	 *                      replace space with underscore. For RDBMS this should be
	 *                      false
	 * 
	 * @return Cleaned string
	 */
	public static String cleanString(String original, boolean replaceForwardSlash, boolean replaceForRDF, boolean property) {
		String retString = original;

		retString = retString.trim();
		retString = retString.replaceAll("\t", " ");// replace tabs with spaces
		while (retString.contains("  ")) {
			retString = retString.replace("  ", " ");
		}
		retString = retString.replaceAll("\\{", "(");
		retString = retString.replaceAll("\\}", ")");
		retString = retString.replace("'", "");// remove apostrophe
		if (replaceForRDF) {
			retString = retString.replaceAll("\"", "'");// replace double quotes with single quotes
		}
		retString = retString.replace(" ", "_");// replace spaces with underscores
		if (!property) {
			if (replaceForwardSlash) {
				retString = retString.replace("/", "-");// replace forward slashes with dashes
			}
			retString = retString.replaceAll("\\\\", "-");// replace backslashes with dashes
		}

		retString = retString.replaceAll("\\|", "-");// replace vertical lines with dashes
		retString = retString.replaceAll("\n", " ");
		retString = retString.replace("<", "(");
		retString = retString.replace(">", ")");

		return retString;
	}

	public static String cleanVariableString(String original) {
		String cleaned = cleanString(original, true);
		cleaned = cleaned.replace(",", "");
		cleaned = cleaned.replace("%", "");
		cleaned = cleaned.replace("-", "");
		cleaned = cleaned.replace("(", "");
		cleaned = cleaned.replace(")", "");
		cleaned = cleaned.replace("&", "and");
		while (cleaned.contains("__")) {
			cleaned = cleaned.replace("__", "_");
		}

		return cleaned;
	}

	public static String makeAlphaNumeric(String s) {
		s = s.trim();
		s = s.replace(" ", "_");
		s = s.replaceAll("[^a-zA-Z0-9\\_]", "");
		while (s.contains("__")) {
			s = s.replace("__", "_");
		}
		return s;
	}

	public static String cleanPredicateString(String original) {
		String cleaned = cleanString(original, true);
		cleaned = cleaned.replaceAll("[()]", "");
		cleaned = cleaned.replace(",", "");
		cleaned = cleaned.replace("?", "");
		cleaned = cleaned.replace("&", "");
		return cleaned;
	}

	/**
	 * Creates an excel workbook
	 * 
	 * @param wb      XSSFWorkbook to write to
	 * @param fileLoc String containing the path to save the workbook
	 */
	public static void writeWorkbook(Workbook wb, String fileLocation) {
		fileLocation = Utility.normalizePath(fileLocation);
		// make sure the directory exists
		{
			File file = new File(fileLocation);
			if(!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}
		
		FileOutputStream newExcelFile = null;
		try {
			newExcelFile = new FileOutputStream(fileLocation);
			wb.write(newExcelFile);
			newExcelFile.flush();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} finally {
			try {
				if (newExcelFile != null) {
					newExcelFile.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
		}
	}

	public static Hashtable<String, Object> getParamsFromString(String params) {
		Hashtable<String, Object> paramHash = new Hashtable<>();
		if (params != null) {
			StringTokenizer tokenz = new StringTokenizer(params, "~");
			while (tokenz.hasMoreTokens()) {
				String thisToken = tokenz.nextToken();
				int index = thisToken.indexOf('$');
				String key = thisToken.substring(0, index);
				String value = thisToken.substring(index + 1);
				// attempt to see if
				boolean found = false;
				try {
					double dub = Double.parseDouble(value);
					paramHash.put(key, dub);
					found = true;
				} catch (RuntimeException ignored) {
					logger.debug(ignored);
				}
				if (!found) {
					try {
						int dub = Integer.parseInt(value);
						paramHash.put(key, dub);
					} catch (RuntimeException ignored) {
						logger.debug(ignored);
					}
				}
				paramHash.put(key, value);
			}
		}
		return paramHash;
	}

	public static String retrieveResult(String api, Hashtable<String, String> params) {
		String output = "";
		BufferedReader stream = null;
		InputStreamReader inputStream = null;
		CloseableHttpClient httpclient = null;
		try {
			URIBuilder uri = new URIBuilder(api);

			logger.debug("Getting data from the API...  " + api);
			logger.debug("Params is " + params);

			SSLContextBuilder builder = new SSLContextBuilder();
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build(),
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

			HttpPost get = new HttpPost(api);
			if (params != null) // add the parameters
			{
				List<NameValuePair> nvps = new ArrayList<>();
				for (Enumeration<String> keys = params.keys(); keys.hasMoreElements();) {
					String key = keys.nextElement();
					String value = params.get(key);
					uri.addParameter(key, cleanHttpResponse(value));
					nvps.add(new BasicNameValuePair(key, value));
				}
				get.setEntity(new UrlEncodedFormEntity(nvps));
				// get = new HttpPost(uri.build());
			}

			CloseableHttpResponse response = httpclient.execute(get);
			HttpEntity entity = response.getEntity();

			if (entity != null) {
				inputStream = new InputStreamReader(entity.getContent());
				stream = new BufferedReader(inputStream);
				String data = null;
				while ((data = stream.readLine()) != null)
					output = output + data;
			}
		} catch (RuntimeException ex) {
			logger.error(Constants.STACKTRACE, ex);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (NoSuchAlgorithmException nsae) {
			logger.error(Constants.STACKTRACE, nsae);
		} catch (KeyStoreException kse) {
			logger.error(Constants.STACKTRACE, kse);
		} catch (URISyntaxException use) {
			logger.error(Constants.STACKTRACE, use);
		} catch (KeyManagementException kme) {
			logger.error(Constants.STACKTRACE, kme);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
				if (stream != null)
					stream.close();
			} catch (IOException e) {
				logger.error("Error closing input stream for image");
			}
			try {
				if (httpclient != null)
					httpclient.close();
				if (stream != null)
					stream.close();
			} catch (IOException e) {
				logger.error("Error closing socket for httpclient");
			}
		}
		if (output.length() == 0)
			output = null;

		return output;
	}

	public static InputStream getStream(String api, Hashtable<String, String> params) {
		HttpEntity entity;
		CloseableHttpClient httpclient = null;
		try {
			URIBuilder uri = new URIBuilder(api);

			logger.info("Getting data from the API...  " + Utility.cleanLogString(api));
			logger.info("Params are " + Utility.cleanLogMap(params, "HASHTABLE"));

			SSLContextBuilder builder = new SSLContextBuilder();
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build(),
					SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

			HttpPost get = new HttpPost(api);
			if (params != null) // add the parameters
			{
				List<NameValuePair> nvps = new ArrayList<>();
				for (Enumeration<String> keys = params.keys(); keys.hasMoreElements();) {
					String key = keys.nextElement();
					String value = params.get(key);
					uri.addParameter(key, value);
					nvps.add(new BasicNameValuePair(key, value));
				}
				get.setEntity(new UrlEncodedFormEntity(nvps));
				// get = new HttpPost(uri.build());
			}

			CloseableHttpResponse response = httpclient.execute(get);
			entity = response.getEntity();
			return entity.getContent();

		} catch (RuntimeException ex) {
			logger.error(Constants.STACKTRACE, ex);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (NoSuchAlgorithmException nsae) {
			logger.error(Constants.STACKTRACE, nsae);
		} catch (KeyStoreException kse) {
			logger.error(Constants.STACKTRACE, kse);
		} catch (URISyntaxException use) {
			logger.error(Constants.STACKTRACE, use);
		} catch (KeyManagementException kme) {
			logger.error(Constants.STACKTRACE, kme);
		}
		return null;
	}

	public static ISelectWrapper processQuery(IEngine engine, String query) {
		logger.debug("PROCESSING QUERY: " + query);

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
	 * 
	 * @param s The value to determine the type off
	 * @return The type of the value
	 */
	public static String processType(String s) {

		if (s == null) {
			return null;
		}

		boolean isDouble = true;
		try {
			double val = Double.parseDouble(s);
			if (val == Math.floor(val)) {
				return "INTEGER";
			}
		} catch (NumberFormatException e) {
			isDouble = false;
		}

		if (isDouble) {
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
		if (isLongDate) {
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
		if (isSimpleDate) {
			return ("SIMPLEDATE");
		}

		return ("STRING");
	}

	public static String[] filterNames(String[] names, boolean[] include) {
		int size = 0;
		for (boolean val : include) {
			if (val) {
				size++;
			}
		}

		String[] newNames = new String[size];
		int nextIndex = 0;
		for (int i = 0; i < names.length; i++) {
			if (include[i]) {
				newNames[nextIndex] = names[i];
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
	 * 
	 * @param raw    TODO
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String> getVectorOfReturn(String query, IEngine engine, Boolean raw) {
		Vector<String> retArray = new Vector<>();
		IRawSelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getRawWrapper(engine, query);
			while (wrap.hasNext()) {
				Object[] values = null;
				if (raw) {
					values = wrap.next().getRawValues();
				} else {
					values = wrap.next().getValues();
				}

				if (values[0] != null) {
					retArray.add(values[0].toString());
				} else {
					retArray.add(null);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrap != null) {
				wrap.cleanUp();
			}
		}

		return retArray;
	}

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * 
	 * @param raw    TODO
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String[]> getVectorArrayOfReturn(String query, IEngine engine, Boolean raw) {
		Vector<String[]> retArray = new Vector<>();
		IRawSelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getRawWrapper(engine, query);
			while (wrap.hasNext()) {
				Object[] values = null;
				if (raw) {
					values = wrap.next().getRawValues();
				} else {
					values = wrap.next().getValues();
				}

				String[] valArray = new String[values.length];
				for (int i = 0; i < values.length; i++) {
					if (values[i] != null) {
						valArray[i] = values[i] + "";
					}
				}
				retArray.add(valArray);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrap != null) {
				wrap.cleanUp();
			}
		}

		return retArray;
	}

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * 
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String[]> getVectorObjectOfReturn(String query, IEngine engine) {
		Vector<String[]> retString = new Vector<>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(engine, query);

		String[] names = wrap.getPhysicalVariables();

		while (wrap.hasNext()) {
			String[] values = new String[names.length];
			ISelectStatement bs = wrap.next();
			for (int i = 0; i < names.length; i++) {
				Object value = bs.getRawVar(names[i]);
				String val = null;
				if (value instanceof Binding) {
					val = ((Value) ((Binding) value).getValue()).stringValue();
				} else {
					val = value + "";
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
		// really simple, I am running the keys and then I will have strip the instance
		// values out
		Iterator<String> keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String singleKey = keys.next();
			List<Object> valueList = paramHash.get(singleKey);
			List<Object> newValuesList = new Vector<>();
			for (int i = 0; i < valueList.size(); i++) {
				String value = valueList.get(i) + "";
				if (value.startsWith("http:") || value.contains(":")) {
					value = getInstanceName(value);
				}
				newValuesList.add(value);
			}
			paramHash.put(singleKey, newValuesList);
		}

		return paramHash;
	}

	public static IPlaySheet getPlaySheet(IEngine engine, String psName) {
		logger.info("Trying to get playsheet for " + psName);
		String psClassName = null;
		if (engine != null) {
			psClassName = engine.getProperty(psName);
		}
		if (psClassName == null) {
			psClassName = (String) DIHelper.getInstance().getLocalProp(psName);
		}
		if (psClassName == null) {
			psClassName = DIHelper.getInstance().getProperty(psName);
		}
		if (psClassName == null) {
			psClassName = PlaySheetRDFMapBasedEnum.getClassFromName(psName);
		}
		if (psClassName == null || psClassName.isEmpty()) {
			psClassName = psName;
		}

		IPlaySheet playSheet = (IPlaySheet) getClassFromString(psClassName);

		return playSheet;
	}

	public static IDataMaker getDataMaker(IEngine engine, String dataMakerName) {
		logger.info("Trying to get data maker for " + dataMakerName);
		String dmClassName = null;
		if (engine != null) {
			dmClassName = engine.getProperty(dataMakerName);
		}
		if (dmClassName == null) {
			dmClassName = (String) DIHelper.getInstance().getLocalProp(dataMakerName);
		}
		if (dmClassName == null) {
			dmClassName = DIHelper.getInstance().getProperty(dataMakerName);
		}
		if (dmClassName == null) {
			dmClassName = PlaySheetRDFMapBasedEnum.getClassFromName(dataMakerName);
		}
		if (dmClassName == null || dmClassName.isEmpty()) {
			dmClassName = dataMakerName;
		}

		IDataMaker dm = (IDataMaker) getClassFromString(dmClassName);

		return dm;
	}

	public static IPlaySheet preparePlaySheet(IEngine engine, String sparql, String psName, String playSheetTitle,
			String insightID) {
		IPlaySheet playSheet = getPlaySheet(engine, psName);
		// QuestionPlaySheetStore.getInstance().put(insightID, playSheet);
		playSheet.setQuery(sparql);
		playSheet.setRDFEngine(engine);
		playSheet.setQuestionID(insightID);
		playSheet.setTitle(playSheetTitle);
		return playSheet;
	}

	public static ISEMOSSTransformation getTransformation(IEngine engine, String transName) {
		logger.info("Trying to get transformation for " + transName);
		String transClassName = (String) DIHelper.getInstance().getLocalProp(transName);
		if (transClassName == null) {
			transClassName = DIHelper.getInstance().getProperty(transName);
		}
		if (transClassName == null || transClassName.isEmpty()) {
			transClassName = transName;
		}

		ISEMOSSTransformation transformation = (ISEMOSSTransformation) getClassFromString(transClassName);
		return transformation;
	}

	public static ISEMOSSAction getAction(IEngine engine, String actionName) {
		logger.info("Trying to get action for " + actionName);
		String actionClassName = (String) DIHelper.getInstance().getLocalProp(actionName);
		if (actionClassName == null) {
			actionClassName = DIHelper.getInstance().getProperty(actionName);
		}
		if (actionClassName == null || actionClassName.isEmpty()) {
			actionClassName = actionName;
		}

		ISEMOSSAction action = (ISEMOSSAction) getClassFromString(actionClassName);
		return action;
	}

	public static Object getClassFromString(String className) {
		Object obj = null;
		try {
			logger.debug("Dataframe name is " + className);
			obj = Class.forName(className).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException cnfe) {
			logger.error(Constants.STACKTRACE, cnfe);
			logger.fatal("No such class: " + className);
		} catch (InstantiationException ie) {
			logger.error(Constants.STACKTRACE, ie);
			logger.fatal("Failed instantiation: " + className);
		} catch (IllegalAccessException iae) {
			logger.error(Constants.STACKTRACE, iae);
			logger.fatal("Illegal Access: " + className);
		} catch (IllegalArgumentException iare) {
			logger.error(Constants.STACKTRACE, iare);
			logger.fatal("Illegal argument: " + className);
		} catch (InvocationTargetException ite) {
			logger.error(Constants.STACKTRACE, ite);
			logger.fatal("Invocation exception: " + className);
		} catch (NoSuchMethodException nsme) {
			logger.error(Constants.STACKTRACE, nsme);
			logger.fatal("No constructor: " + className);
		} catch (SecurityException se) {
			logger.error(Constants.STACKTRACE, se);
			logger.fatal("Security exception: " + className);
		}
		return obj;
	}

	public static String getKeyFromValue(Map<String, String> hm, String value) {
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
	 * Return Object[] with prediction of the data type Index 0 -> return the casted
	 * object Index 1 -> return the pixel data type Index 2 -> return the additional
	 * formatting
	 * 
	 * @param input
	 * @return
	 */
	public static Object[] determineInputType(String input) {
		Object[] retObject = new Object[3];
		if (input != null) {
			Object retO = null;
			// is it a boolean ?
			if (input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
				retObject[0] = Boolean.parseBoolean(input);
				retObject[1] = SemossDataType.BOOLEAN;
			}
			// is it a date ?
			else if ((retO = SemossDate.genDateObj(input)) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.DATE;
				retObject[2] = ((SemossDate) retO).getPattern();
			}
			// is it a timestamp ?
			else if ((retO = SemossDate.genTimeStampDateObj(input)) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.TIMESTAMP;
				retObject[2] = ((SemossDate) retO).getPattern();
			}
			// is it an integer ?
			else if ((retO = getInteger(input.replaceAll("[$,\\s]", ""))) != null) {
				retObject[0] = retO;
				retObject[1] = SemossDataType.INT;
			}
			// is it a double ?
			else if ((retO = getDouble(input.replaceAll("[$,\\s]", ""))) != null) {
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
	public static Object[] findTypes(String input) {
		// System.out.println("String that came in.. " + input);
		Object[] retObject = null;
		if (input != null) {
			Object retO = null;
			if (input.equalsIgnoreCase("true") || input.equalsIgnoreCase("false")) {
				retObject = new Object[2];
				retObject[0] = "boolean";
				retObject[1] = retO;

			}
			// all numbers are
			// else if(NumberUtils.isDigits(input))
			// {
			// retO = Integer.parseInt(input);
			// retObject = new Object[2];
			// retObject[0] = "int";
			// retObject[1] = retO;
			// }

			else if ((retO = getDate(input)) != null)// try dates ? - yummy !!
			{
				retObject = new Object[2];
				retObject[0] = "date";
				retObject[1] = retO;

			} else if ((retO = getDouble(input)) != null) {
				retObject = new Object[2];
				retObject[0] = "double";
				retObject[1] = retO;
			} else if ((retO = getCurrency(input)) != null) {

				retObject = new Object[2];
				if (retO instanceof String)
					retObject[0] = "varchar(800)";
				else
					retObject[0] = "double";
				retObject[1] = retO;
			} else {
				retObject = new Object[2]; // need to do some more stuff to determine this
				retObject[0] = "varchar(800)";
				retObject[1] = input;
			}
		}
		return retObject;
	}

	@Deprecated
	public static String getDate(String input) {
		String[] date_formats = {
				// year, month, day
				"yyyy-MM-dd", "yyyy-MM-d", "yyyy-M-dd", "yyyy-M-d",
				// day, month, year
				"dd-MM-yyyy", "d-MM-yyyy", "dd-M-yyyy", "d-M-yyyy",
				// year / month / day
				"yyyy/MM/dd", "yyyy/MM/d", "yyyy/M/dd", "yyyy/M/d",
				// day, month, year
				"dd/MM/yyyy", "d/MM/yyyy", "dd/M/yyyy", "d/M/yyyy",

				// Abrev. month, day, year
				"MMM_dd,_yyyy", "MMM_d,_yyyy", "MMM dd, yyyy", "MMM d, yyyy",

				// "dd/MM/yyyy",
				"MM/dd/yyyy", "yyyy/MM/dd", "yyyy MMM dd", "yyyy dd MMM", "dd MMM yyyy", "dd MMM", "MMM dd yyyy",
				"MMM dd, yyyy", "MMM dd", "dd MMM yyyy", "MMM yyyy", "dd/MM/yyyy" };

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(formatString);
				Date mydate = sdf.parse(input);
				SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd");
				output_date = outdate.format(mydate);
				itsDate = true;
				break;
			} catch (ParseException e) {
				// System.out.println("Next!");
			}
		}

		return output_date;
	}

	@Deprecated
	public static String getTimeStamp(String input) {
		String[] date_formats = {
				// year, month, day
				"yyyy-MM-dd hh:mm:ss", "yyyy-MM-d hh:mm:ss", "yyyy-M-dd hh:mm:ss", "yyyy-M-d hh:mm:ss",
				"yyyy-MM-dd'T'hh:mm:ss'Z'", "yyyy-MM-d'T'hh:mm:ss'Z'", "yyyy-M-dd'T'hh:mm:ss'Z'",
				"yyyy-M-d'T'hh:mm:ss'Z'", };

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats) {
			try {
				Date mydate = new SimpleDateFormat(formatString).parse(input);
				SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				output_date = outdate.format(mydate);
				itsDate = true;
				break;
			} catch (ParseException e) {
				// System.out.println("Next!");
			}
		}

		return output_date;
	}

	@Deprecated
	public static Date getDateAsDateObj(String input) {
		SimpleDateFormat outdate_formatter = new SimpleDateFormat("yyyy-MM-dd");
		String output_date = getDate(input);
		if (output_date == null) {
			return null;
		}

		Date outDate = null;
		try {
			outDate = outdate_formatter.parse(output_date);
		} catch (ParseException e) {
//			logger.error(Constants.STACKTRACE, e);
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
			logger.error(Constants.STACKTRACE, e);
		}
		return mydate;
	}

	@Deprecated
	public static Date getTimeStampAsDateObj(String input) {
		SimpleDateFormat outdate_formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.ssss");
		String output_date = getTimeStamp(input);
		if (output_date == null) {
			return null;
		}

		Date outDate = null;
		try {
			outDate = outdate_formatter.parse(output_date);
		} catch (ParseException e) {
//			logger.error(Constants.STACKTRACE, e);
		}

		return outDate;
	}

	@Deprecated
	public static Object getCurrency(String input) {
		// COMMENTING THIS OUT BECAUSE CAST TO TYPES BREAKS IN CASES WHERE THIS RETURNS,
		// NEED TO UPDATE THAT BUT WILL KEEP IT AS STRING FOR NOW
		// what is this check???
		// this is messing up the types since it works based on if there is a null
		// pointer
		// if(input.matches("\\Q$\\E(\\d+)\\Q.\\E?(\\d+)?\\Q-\\E\\Q$\\E(\\d+)\\Q.\\E?(\\d+)?"))
		// {
		// return input;
		// }
		// Number nm = null;
		// NumberFormat nf = NumberFormat.getCurrencyInstance();
		// try {
		// nm = nf.parse(input);
		// //System.out.println("Curr.. " + nm);
		// }catch (Exception ex)
		// {
		//
		// }
		// return nm;
		return null;
	}

	public static Double getDouble(String input) {
		// try to do some basic clean up if it fails and try again
		try {
			if (input.startsWith("(") && input.endsWith(")")) {
				input = "-" + input.substring(1, input.length() - 1);
			}
			Double num = Double.parseDouble(input);
			return num;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public static Integer getInteger(String input) {
		// try to do some basic clean up if it fails and try again
		try {
			if (input.startsWith("(") && input.endsWith(")")) {
				input = "-" + input.substring(1, input.length() - 1);
			}
			Integer num = Integer.parseInt(input);
			return num;
		} catch (NumberFormatException e) {
			Double db = getDouble(input);
			if (db != null && db == db.intValue()) {
				return db.intValue();
			}
			return null;
		}
	}

	// this doesn't consider 1.2E8 etc.
	// public static boolean isNumber(String input) {
	// //has digits, followed by optional period followed by digits
	// return input.matches("(\\d+)\\Q.\\E?(\\d+)?");
	// }

	@Deprecated
	public static String[] castToTypes(String[] thisOutput, String[] types) {
		String[] values = new String[thisOutput.length];

		for (int outIndex = 0; outIndex < thisOutput.length; outIndex++) {
			// if the value is not null
			if (thisOutput[outIndex] != null && thisOutput[outIndex].length() > 0) {
				values[outIndex] = thisOutput[outIndex] + "";

				if (thisOutput[outIndex] != null) // && castTargets.contains(outIndex + ""))
				{
					if (types[outIndex].equalsIgnoreCase("Date"))
						values[outIndex] = getDate(thisOutput[outIndex]);
					else if (types[outIndex].equalsIgnoreCase("Currency"))// this is a currency
						values[outIndex] = getCurrency(thisOutput[outIndex]) + "";
					else if (types[outIndex].equalsIgnoreCase("varchar(800)")) {
						if (thisOutput[outIndex].length() >= 800)
							thisOutput[outIndex] = thisOutput[outIndex].substring(0, 798);
						values[outIndex] = thisOutput[outIndex];
					}
				}
			} else if (types[outIndex] != null) {
				if (types[outIndex].equalsIgnoreCase("Double"))
					values[outIndex] = "NULL";
				else if (types[outIndex].equalsIgnoreCase("varchar(800)") || types[outIndex].equalsIgnoreCase("date"))
					values[outIndex] = "";
			} else {
				values[outIndex] = "";
			}
		}

		for (int i = 0; i < values.length; i++) {
			values[i] = Utility.cleanString(values[i], true, true, false);
		}
		return values;
	}

	@Deprecated
	public static String castToTypes(String thisOutput, String type) {
		String values = "";

		if (thisOutput != null && thisOutput.length() > 0) {
			values = thisOutput + "";

			if (thisOutput != null) // && castTargets.contains(outIndex + ""))
			{
				if (type.equalsIgnoreCase("Date"))
					values = getDate(thisOutput);
				else if (type.equalsIgnoreCase("Currency"))// this is a currency
					values = getCurrency(thisOutput) + "";
				else if (type.equalsIgnoreCase("varchar(800)")) {
					if (thisOutput.length() >= 800)
						thisOutput = thisOutput.substring(0, 798);
					values = thisOutput;
				}
			}
		} else if (type != null) {
			if (type.equalsIgnoreCase("Double"))
				values = "NULL";
			else if (type.equalsIgnoreCase("varchar(800)") || type.equalsIgnoreCase("date"))
				values = "";
		} else {
			values = "";
		}
		return values;
	}

	public static Map<Integer, Set<Integer>> getCardinalityOfValues(String[] newHeaders,
			Map<String, Set<String>> edgeHash) {
		Map<Integer, Set<Integer>> retMapping = new Hashtable<>();

		if (edgeHash == null) {
			return retMapping;
		}

		for (String startNode : edgeHash.keySet()) {
			Integer startIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(newHeaders, startNode);

			// for nulls and stuff
			Set<String> set = edgeHash.get(startNode);
			if (set == null) {
				continue;
			}

			// finish the mappings
			for (String endNode : set) {
				Integer endIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(newHeaders, endNode);

				// add mapping
				if (!retMapping.containsKey(startIndex)) {
					Set<Integer> downstream = new HashSet<>();
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
		if (cleanDataType == null || cleanDataType.isEmpty()) {
			return "VARCHAR(800)";
		}
		cleanDataType = cleanDataType.toUpperCase();

		if (cleanDataType.equals("STRING")) {
			return "VARCHAR(800)";
		}

		// currently send double and date, which are the same as raw data type
		return cleanDataType;
	}

	public static String getCleanDataType(String origDataType) {
		if (origDataType == null || origDataType.isEmpty()) {
			return "STRING";
		}
		origDataType = origDataType.toUpperCase();

		if (isIntegerType(origDataType) || isDoubleType(origDataType)) {
			return "NUMBER";
		}

		if (isDateType(origDataType) || isTimeStamp(origDataType)) {
			return "DATE";
		}

		if (isStringType(origDataType)) {
			return "STRING";
		}

		return "STRING";
	}

	public static String getH2DataType(String dataType) {
		if (isH2DataType(dataType)) {
			return dataType;
		}

		String returnType = getH2TypeConversionMap().get(dataType);

		return returnType;
	}

	public static boolean isH2DataType(String dataType) {
		if (
		// INT TYPE
		dataType.equals("INT") || dataType.equals("INTEGER") || dataType.equals("MEDIUMINT") || dataType.equals("INT4")
				|| dataType.equals("SIGNED")

				// BOOLEAN TYPE
				|| dataType.equals("BOOLEAN") || dataType.equals("BIT") || dataType.equals("BOOL")

				// TINYINT TYPE
				|| dataType.equals("TINYINT")

				// SMALLINT TYPE
				|| dataType.equals("SMALLINT") || dataType.equals("INT2") || dataType.equals("YEAR")

				// BIGINT TYPE
				|| dataType.equals("BIGINT") || dataType.equals("INT8")

				// IDENTITY TYPE
				|| dataType.equals("IDENTITY")

				// DECIMAL TYPE
				|| dataType.equals("DECIMAL") || dataType.equals("NUMBER") || dataType.equals("DEC")
				|| dataType.equals("NUMERIC")

				// DOUBLE TYPE
				|| dataType.equals("DOUBLE") || dataType.equals("PRECISION") || dataType.equals("FLOAT")
				|| dataType.equals("FLOAT8")

				// REAL TYPE
				|| dataType.equals("REAL") || dataType.equals("FLOAT4")

				// TIME TYPE
				|| dataType.equals("TIME")

				// DATE TYPE
				|| dataType.equals("DATE")

				// TIMESTAMP TYPE
				|| dataType.equals("TIMESTAMP") || dataType.equals("DATETIME") || dataType.equals("SMALLDATETIME")

				// BINARY TYPE
				|| dataType.startsWith("BINARY") || dataType.startsWith("VARBINARY")
				|| dataType.startsWith("LONGVARBINARY") || dataType.startsWith("RAW") || dataType.startsWith("BYTEA")

				// OTHER TYPE
				|| dataType.equals("OTHER")

				// VARCHAR TYPE
				|| dataType.startsWith("VARCHAR") || dataType.startsWith("LONGVARCHAR")
				|| dataType.startsWith("VARCHAR2") || dataType.startsWith("NVARCHAR")
				|| dataType.startsWith("NVARCHAR2") || dataType.startsWith("VARCHAR_CASESENSITIVE")

				// VARCHAR_IGNORECASE TYPE
				|| dataType.startsWith("VARCHAR_IGNORECASE")

				// CHAR TYPE
				|| dataType.startsWith("CHAR") || dataType.startsWith("CHARACTER") || dataType.startsWith("NCHAR")

				// BLOB TYPE
				|| dataType.equals("BLOB") || dataType.equals("TINYBLOB") || dataType.equals("MEDIUMBLOB")
				|| dataType.equals("LONGBLOB") || dataType.equals("IMAGE") || dataType.equals("OID")

				// CLOG TYPE
				|| dataType.equals("CLOB") || dataType.equals("TINYTEXT") || dataType.equals("TEXT")
				|| dataType.equals("MEDIUMTEXT") || dataType.equals("NTEXT") || dataType.equals("NCLOB")

				// UUID TYPE
				|| dataType.equals("UUID")

				// ARRAY TYPE
				|| dataType.equals("ARRAY")

				// GEOMETRY TYPE
				|| dataType.equals("GEOMETRY")

		) {
			return true;
		}
		return false;
	}

	public static boolean isNumericType(String dataType) {
		if (isIntegerType(dataType) || isDoubleType(dataType)) {
			return true;
		}

		return false;
	}

	public static boolean isIntegerType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		if (dataType.startsWith("IDENTITY") || dataType.startsWith("LONG") || dataType.startsWith("INT")
				|| dataType.startsWith("INTEGER") || dataType.startsWith("MEDIUMINT") || dataType.startsWith("INT4")
				|| dataType.startsWith("SIGNED")

				// TINYINT TYPE
				|| dataType.startsWith("TINYINT")

				// SMALLINT TYPE
				|| dataType.startsWith("SMALLINT") || dataType.startsWith("INT2") || dataType.startsWith("YEAR")

				// BIGINT TYPE
				|| dataType.startsWith("BIGINT") || dataType.startsWith("INT8")

				// PANDAS
				|| dataType.contains("DTYPE('INT64')")) {
			return true;
		}

		return false;
	}

	public static boolean isBoolean(String dataType) {
		dataType = dataType.toUpperCase().trim();
		if (dataType.startsWith("BOOL") || dataType.startsWith("BIT")) {
			return true;
		}

		return false;
	}

	public static boolean isDoubleType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		if (dataType.startsWith("NUMBER") || dataType.startsWith("MONEY") || dataType.startsWith("SMALLMONEY")
				|| dataType.startsWith("FLOAT")

				// DECIMAL TYPE
				|| dataType.startsWith("DECIMAL") || dataType.startsWith("NUMBER") || dataType.startsWith("DEC")
				|| dataType.startsWith("NUMERIC")

				// DOUBLE TYPE
				|| dataType.startsWith("DOUBLE") || dataType.startsWith("PRECISION") || dataType.startsWith("FLOAT")
				|| dataType.startsWith("FLOAT8")

				// REAL TYPE
				|| dataType.startsWith("REAL") || dataType.startsWith("FLOAT4")

				// PANDAS
				|| dataType.contains("DTYPE('FLOAT64')")

		) {
			return true;
		}

		return false;
	}

	public static boolean isStringType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		if (dataType.equals("STRING")
				// VARCHAR TYPE
				|| dataType.startsWith("VARCHAR") || dataType.startsWith("TEXT") || dataType.startsWith("LONGVARCHAR")
				|| dataType.startsWith("VARCHAR2") || dataType.startsWith("NVARCHAR")
				|| dataType.startsWith("NVARCHAR2") || dataType.startsWith("VARCHAR_CASESENSITIVE")

				// VARCHAR_IGNORECASE TYPE
				|| dataType.startsWith("VARCHAR_IGNORECASE")

				// CHAR TYPE
				|| dataType.startsWith("CHAR") || dataType.startsWith("CHARACTER") || dataType.startsWith("NCHAR")

				// R TYPE
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

		return dataType.equals("DATE");
	}

	public static boolean isTimeStamp(String dataType) {
		dataType = dataType.toUpperCase().trim();

		return dataType.startsWith("TIMESTAMP") || dataType.startsWith("DATETIME");
	}

	// return the translation from sql types to h2 types
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
	 * Take the file location and return the original file name Based on upload
	 * flow, files that go through FileUploader.java class get appended with the
	 * date of upload in the format of "_yyyy_MM_dd_HH_mm_ss_SSSS" (length of 25)
	 * Thus, we see if the file has the date appended and remove it if we find it
	 * 
	 * @param FILE_LOCATION The location of the file
	 * @param EXTENSION     The file extension
	 * @return The original file name
	 */
	public static String getOriginalFileName(final String FILE_LOCATION) {
		// The FileUploader appends the time as "_yyyy_MM_dd_HH_mm_ss_SSSS"
		// onto the original fileName in order to ensure that it is unique
		// since we are using the fileName to be the table name, let us try and remove
		// this
		String ext = "." + FilenameUtils.getExtension(FILE_LOCATION);
		String fileName = FilenameUtils.getName(FILE_LOCATION).replace(ext, "");
		// 24 is the length of the date being added
		if (fileName.length() > 28) {
			String fileEnd = fileName.substring(fileName.length() - 24);
			try {
				new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").parse(fileEnd);
				// if the parsing was successful, we remove it from the fileName
				fileName = fileName.substring(0, fileName.length() - 24);
			} catch (ParseException e) {
				// the end was not the added date, so do nothing
			}
		}
		return fileName + ext;
	}

	/**
	 * Loads an engine - sets the core properties, loads base data engine and
	 * ontology file.
	 * 
	 * @param Filename.
	 * @param List      of properties.
	 * 
	 * @return Loaded engine.
	 */
	public static IEngine loadEngine(String fileName, Properties prop) {
		IEngine engine = null;
		try {
			String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
			String engineId = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);

			if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId)) {
				logger.debug("DB " + engineId + " is already loaded...");
				// engines are by default loaded so that we can keep track on the front end of
				// engine/all call
				// so even though it is added here there is a good possibility it is not loaded
				// so check to see this
				if (DIHelper.getInstance().getLocalProp(engineId) instanceof IEngine) {
					return (IEngine) DIHelper.getInstance().getLocalProp(engineId);
				}
			}

			// we store the smss location in DIHelper
			DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.STORE, fileName);
			// we also store the OWL location
			if (prop.containsKey(Constants.OWL)) {
				DIHelper.getInstance().getCoreProp().setProperty(engineId + "_" + Constants.OWL,
						prop.getProperty(Constants.OWL));
			}

			// create and open the class
			engine = (IEngine) Class.forName(engineClass).newInstance();
			engine.setEngineId(engineId);
			engine.openDB(fileName);

			// set the engine in DIHelper
			DIHelper.getInstance().setLocalProperty(engineId, engine);

			// Append the engine name to engines if not already present
			if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId))) {
				engines = engines + ";" + engineId;
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			}

			boolean isLocal = engineId.equals(Constants.LOCAL_MASTER_DB_NAME);
			boolean isSecurity = engineId.equals(Constants.SECURITY_DB);
			boolean isThemes = engineId.equals(Constants.THEMING_DB);
			if (!isLocal && !isSecurity && !isThemes) {
				// sync up the engine metadata now
				synchronizeEngineMetadata(engineId);
				SecurityUpdateUtils.addApp(engineId);
			}
		} catch (InstantiationException ie) {
			logger.error(Constants.STACKTRACE, ie);
		} catch (IllegalAccessException iae) {
			logger.error(Constants.STACKTRACE, iae);
		} catch (ClassNotFoundException cnfe) {
			logger.error(Constants.STACKTRACE, cnfe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
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
		if (localMaster == null) {
			logger.info(">>>>>>>> Unable to find local master database in DIHelper.");
			return;
		}

		// generate the appropriate query to execute on the local master engine to get
		// the time stamp
		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(engineId + "_" + Constants.STORE);

		// this has all the details
		// the engine file is primarily the SMSS that is going to be utilized for the
		// purposes of retrieving all the data
		Properties prop = Utility.loadProperties(smssFile);

		String rawType = prop.get(Constants.ENGINE_TYPE).toString();
		if (rawType.contains("AppEngine")) {
			// this engine has no data! it is just a collection of insights
			// nothing to synchronize into local master
			return;
		}

		// TODO: NEED TO STILL BUILD THIS OUT!
		if (rawType.contains("RemoteSemossEngine")) {
			return;
		}

		AddToMasterDB adder = new AddToMasterDB();

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date rdbmsDate = adder.getEngineDate(engineId);
		File owlFile = SmssUtilities.getOwlFile(prop);
		String engineDbTime = df.format(new Date(owlFile.lastModified()));

		// 4) perform the necessary additions if the time stamps do not equal
		// this is broken out into 2 separate parts
		// 4.1) the local master doesn't have a time stamp which means the engine is not
		// present
		// -> i.e. we do not need to remove the engine and re-add it
		// 4.2) the time is present and we need to remove anything relating the engine
		// that was in the engine and then re-add it
		String engineRdbmsDbTime = "Dummy";
		if (rdbmsDate != null) {
			engineRdbmsDbTime = df.format(rdbmsDate);
		}

		if (rdbmsDate == null) {
			// logic to register the engine into the local master
			adder.registerEngineLocal(prop);
			adder.commit(localMaster);
		} else if (!engineRdbmsDbTime.equalsIgnoreCase(engineDbTime)) {
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
	 * Splits up a URI into tokens based on "/" character and uses logic to return
	 * the instance name.
	 * 
	 * @param String URI to be split into tokens.
	 * 
	 * @return String Instance name.
	 */
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

		if (type == IEngine.ENGINE_TYPE.RDBMS || type == IEngine.ENGINE_TYPE.R)
			instanceName = "Table_" + instanceName + "Column_" + secondLastToken;

		return instanceName;
	}

	public static String getRandomString(int len) {
		String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";

		String retString = "a";
		for (int i = 0; i < len; i++) {
			double num = Math.random() * alpha.length();
			retString = retString + alpha.charAt(new Double(num).intValue());
		}

		return retString;
	}

	public static boolean engineLoaded(String engineId) {
		return DIHelper.getInstance().getLocalProp(engineId) != null;
	}

	/**
	 * 
	 * @param engineId - engine to get
	 * @return
	 * 
	 *         Use this method to get the engine when the engine hasn't been loaded
	 */
	public static IEngine getEngine(String engineId) {
		return getEngine(engineId, true, false);
	}

	/**
	 * 
	 * @param engineId - engine to get
	 * @return
	 * 
	 *         Use this method to get the engine when the engine hasn't been loaded
	 */
	public static IEngine getEngine(String engineId, boolean pullIfNeeded, boolean bypass) {
		if (!bypass) {
			AppAvailabilityStore availableAppStore = AppAvailabilityStore.getInstance();
			if (availableAppStore != null) {
				if (availableAppStore.isAppDisabledByOwner(engineId)) {
					throw new SemossPixelException("The app you are trying to access is currently disabled.  Please reach out to the owner for more details");
				}
			}
		}
		IEngine engine = null;

		// If the engine has already been loaded, then return it
		// Don't acquire the lock here, because that would slow things down
		if (DIHelper.getInstance().getLocalProp(engineId) != null) {
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineId);
		} else {
			// Acquire the lock on the engine,
			// don't want several calls to try and load the engine at the same
			// time
			logger.info("Applying lock for " + engineId + " to push app");
			ReentrantLock lock = EngineSyncUtility.getEngineLock(engineId);
			lock.lock();
			logger.info("App "+ engineId + " is locked");

			try {
				// Need to do a double check here,
				// so if a different thread was waiting for the engine to load,
				// it doesn't go through this process again
				if (DIHelper.getInstance().getLocalProp(engineId) != null) {
					return (IEngine) DIHelper.getInstance().getLocalProp(engineId);
				}
				
				// If in a clustered environment, then pull the app first
				// TODO >>>timb: need to pull sec and lmd each time. They also need
				// correct jdbcs...
				if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
					try {
						CloudClient.getClient().pullApp(engineId);
					} catch (IOException | InterruptedException e) {
						logger.error(Constants.STACKTRACE, e);
						return null;
					}
				}

				// Now that the app has been pulled, grab the smss file
				String smssFile = (String) DIHelper.getInstance().getCoreProp()
						.getProperty(engineId + "_" + Constants.STORE);

				// Start up the engine using the details in the smss
				if (smssFile != null) {
					// actual load engine process
					engine = Utility.loadEngine(smssFile, Utility.loadProperties(smssFile));
				} else {
					logger.debug("There is no SMSS File for the engine " + engineId + "...");
				}

				// TODO >>>timb: Centralize this ZK env check stuff and use is cluster variable
				// TODO >>>timb: remove node exists error or catch it
				// TODO >>>cluster: tag
				// Start with because the insights RDBMS has the id security_InsightsRDBMS
				if (!(engineId.startsWith("security") || engineId.startsWith("LocalMasterDatabase")
						|| engineId.startsWith("form_builder_engine") || engineId.startsWith("themes") || engineId.startsWith("scheduler"))) {
					Map<String, String> envMap = System.getenv();
					if (envMap.containsKey(ZKClient.ZK_SERVER)
							|| envMap.containsKey(ZKClient.ZK_SERVER.toUpperCase())) {
						if (ClusterUtil.LOAD_ENGINES_LOCALLY) {

							// Only publish if actually loading on this box
							// TODO >>>timb: this logic only works insofar as we are assuming a user-based
							// docker layer in addition to the app containers
							String host = "unknown";

							if (envMap.containsKey(ZKClient.HOST))
								host = envMap.get(ZKClient.HOST);

							if (envMap.containsKey(ZKClient.HOST.toUpperCase()))
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
				logger.info("App "+ engineId + " is unlocked");
			}
		}

		return engine;
	}

	public static HashMap<String, Object> getPKQLInputVar(String param, String reactor) {
		HashMap<String, Object> inputMap = new HashMap<>();
		Object restrictions = new Object();

		switch (param) {
		case "COL_DEF":
			inputMap.put("dataType", "column");
			inputMap.put("restrictions", restrictions);
			inputMap.put("source", "");
			switch (reactor) {// COL_DEF specifies different var for some reactors - for COL_ADD its new
								// column name, for COL_SPLIT, its existinmg column name
			case "COL_ADD":
				inputMap.put("label", "New Column Name");
				inputMap.put("varName", "c:newCol");
				inputMap.put("type", "freetext");
				inputMap.put("values", "");
				break;

			case "COL_SPLIT":
				inputMap.put("label", "Column to be split");
				inputMap.put("varName", "c:col1");
				inputMap.put("type", "dropdown");
				inputMap.put("values", "");
				break;

			case "UNFILTER_DATA":
				inputMap.put("label", "Column to be unfiltered");
				inputMap.put("varName", "c:col1");
				inputMap.put("type", "dropdown");
				inputMap.put("values", "");
				break;

			}
			break;

		case "EXPR_TERM":
			inputMap.put("label", "New Column Value");
			inputMap.put("varName", "expression");
			inputMap.put("dataType", "expression");
			inputMap.put("type", "freetext");
			inputMap.put("restrictions", restrictions);
			inputMap.put("source", "");
			break;

		case "WORD_OR_NUM":
			inputMap.put("dataType", "text");
			inputMap.put("restrictions", restrictions);
			inputMap.put("source", "");
			switch (reactor) {
			case "COL_SPLIT":
				inputMap.put("label", "Delimiter");
				inputMap.put("varName", "delimiter");
				inputMap.put("type", "freetext");
				break;
			}
			break;

		case "FILTERS":
			inputMap.put("label", "Column with unfiltered data");
			inputMap.put("varName", "c:col1=[instances]");
			inputMap.put("dataType", "column");
			inputMap.put("type", "filterDropdown");
			inputMap.put("restrictions", restrictions);
			inputMap.put("source", "");
			inputMap.put("values", "");
			break;

		default:
			break;
		}
		return inputMap;
	}

	public static String findOpenPort() {
		logger.info("Finding an open port.. ");
		boolean found = false;

		int lowPort = 5355;
		int highPort = lowPort + 10_000;

		if (DIHelper.getInstance().getProperty("LOW_PORT") != null) {
			try {lowPort = Integer.parseInt(DIHelper.getInstance().getProperty("LOW_PORT")); } catch (Exception ignore) {};
		}
		
		if (DIHelper.getInstance().getProperty("HIGH_PORT") != null) {
			try {highPort = Integer.parseInt(DIHelper.getInstance().getProperty("HIGH_PORT")); } catch (Exception ignore) {};
		}
		
		for (; !found && lowPort < highPort; lowPort++) {
			logger.info("Trying port = " + lowPort);
			try {
				ServerSocket s = new ServerSocket(lowPort); 
				logger.info("Success with port = " + lowPort);
				// no error, found an open port, we can stop
				found = true;
				s.close();
				break;
			} catch (Exception ex) {
				// do nothing
				logger.info("Port " + lowPort + " Failed. " + ex.getMessage());
				found = false;
//				logger.error(Constants.STACKTRACE, ex);
			}
		}

		// if we found a port, return that port
		if (found) {
			return lowPort + "";
		}
		
		// no available ports in the range, either config is bad or something else is messed up
		// just throw an exception
		throw new IllegalArgumentException("Could not find available port to connect to");
	}

	/**
	 * Write an iterator to a file location using "," as a separator
	 * 
	 * @param fileLocation
	 * @param it
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it) {
		return Utility.writeResultToFile(fileLocation, it, null, ",");
	}

	/**
	 * Write an iterator to a file location using the specified separator
	 * 
	 * @param fileLocation
	 * @param it
	 * @param separator
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, String separator) {
		return Utility.writeResultToFile(fileLocation, it, null, separator);
	}

	/**
	 * Write an iterator to a file location using the types map defined and using a
	 * "," as a separator
	 * 
	 * @param fileLocation
	 * @param it
	 * @param typesMap
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it,
			Map<String, SemossDataType> typesMap) {
		return Utility.writeResultToFile(fileLocation, it, typesMap, ",");
	}

	/**
	 * Write a task to a file using a "," as a separator
	 * 
	 * @param fileLocation
	 * @param task
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, ITask task) {
		return writeResultToFile(fileLocation, task, ",");
	}

	/**
	 * Write a task toa file using the specified separator
	 * 
	 * @param fileLocation
	 * @param task
	 * @param seperator
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, ITask task, String seperator) {
		List<Map<String, Object>> headersInfo = task.getHeaderInfo();
		Map<String, SemossDataType> typesMap = new HashMap<>();
		for (Map<String, Object> headerMap : headersInfo) {
			String name = (String) headerMap.get("alias");
			SemossDataType type = SemossDataType.convertStringToDataType(headerMap.get("type").toString());
			headerMap.put(name, type);
		}

		return Utility.writeResultToFile(fileLocation, task, typesMap, seperator);
	}

	public static String adjustTypeR(String frameName, String[] columns, Map<String, SemossDataType> typeMap) {
		StringBuilder adjustTypes = new StringBuilder();
		for (int headIndex = 0; headIndex < columns.length; headIndex++) {
			SemossDataType type = typeMap.get(columns[headIndex]);
			String asType = null;
			if (type == SemossDataType.INT)
				asType = "as.integer(";
			else if (type == SemossDataType.DOUBLE)
				asType = "as.double(";
			if (asType != null)
				adjustTypes.append(frameName).append("$").append(columns[headIndex]).append(" <- ").append(asType)
						.append(frameName).append("$").append(columns[headIndex]).append(");");
		}

		return adjustTypes.toString();
	}

	public static String adjustTypePy(String frameName, String[] columns, Map<String, SemossDataType> typeMap) {
		StringBuilder adjustTypes = new StringBuilder();
		for (int headIndex = 0; headIndex < columns.length; headIndex++) {
			SemossDataType type = typeMap.get(columns[headIndex]);
			String asType = null;
			if (type == SemossDataType.INT)
				asType = "int64";
			else if (type == SemossDataType.DOUBLE)
				asType = "float64";
			if (asType != null)
				adjustTypes.append(frameName).append("['").append(columns[headIndex]).append("']").append(" = ")
						.append(frameName).append("['").append(columns[headIndex]).append("']").append(".astype('")
						.append(asType).append("', errors='ignore')\n");
		}

		return adjustTypes.toString();
	}

	/*
		public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap, String seperator, int parallel) {
			
			fileLocation = fileLocation.replace(".tsv", "");
			
			File fileLoc = new File(fileLocation);
			if(fileLoc.exists())
				fileLoc.delete();
			fileLoc.mkdir();
			
			if(parallel == -1)
				parallel = 10; // defaulting to ten threads
			
			// result gatherer thread that is invoked after all threads are done
			ResultGathererThread rgt = new ResultGathererThread();
			Object daLock = new Object();
			rgt.daLock = daLock;
			
			
			CyclicBarrier cyb = new CyclicBarrier(parallel, new Thread(rgt));
			for(int parIndex = 0;parIndex < parallel;parIndex++)
			{
				ResultWriterThread rwt = new ResultWriterThread();
				rwt.cyb = cyb;
				rwt.fileLocation = fileLocation;
				rwt.it = it;
				rwt.typesMap = typesMap;
				rwt.seperator = seperator;
				rwt.suffix = parIndex+"";
				Thread rwThread = new Thread(rwt);
				rwThread.start();
			}
			
			// need a lock to sleep here but.. 
			synchronized(daLock)
			{
				try {
					daLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			return fileLoc;
		}
	
		*/

	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it,
			Map<String, SemossDataType> typesMap, String seperator, IStringExportProcessor... exportProcessors) {
		long start = System.currentTimeMillis();

		// make sure file is empty so we are only inserting the new values
		File f = new File(fileLocation);
		if (f.exists()) {
			logger.debug("File currently exists.. deleting file");
			f.delete();
		}
		try {
			f.createNewFile();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
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
			if (it.hasNext()) {
				IHeadersDataRow row = it.next();

				// generate the header row
				// and define constants used throughout like size, and types
				i = 0;
				headers = row.getHeaders();
				size = headers.length;
				typesArr = new SemossDataType[size];
				builder = new StringBuilder();
				for (; i < size; i++) {
					builder.append("\"").append(headers[i]).append("\"");
					if ((i + 1) != size) {
						builder.append(seperator);
					}
					if (typesMap == null) {
						typesArr[i] = SemossDataType.STRING;
					} else {
						typesArr[i] = typesMap.get(headers[i]);
						if (typesArr[i] == null) {
							typesArr[i] = SemossDataType.STRING;
						}
					}
				}
				// write the header to the file
				bufferedWriter.write(builder.append("\n").toString());

				// generate the data row
				Object[] dataRow = row.getValues();
				builder = new StringBuilder();
				i = 0;
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							builder.append("\"\"");
						} else {
							String thisStringVal = dataRow[i] + "";
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							builder.append("\"").append(thisStringVal).append("\"");
						}
					} else {
						// print out null
						if(dataRow[i] == null) {
							builder.append("null");
						} else {
							builder.append(dataRow[i]);
						}
					}
					// add sep between columns
					if ((i + 1) != size) {
						builder.append(seperator);
					}
				}
				// write row to file
				bufferedWriter.write(builder.append("\n").toString());
			} else {
				// we have no rows... can we at least export an empty file with headers?
				if(it instanceof IRawSelectWrapper) {
					i = 0;
					headers = ((IRawSelectWrapper) it).getHeaders();
					size = headers.length;
					builder = new StringBuilder();
					for (; i < size; i++) {
						builder.append("\"").append(headers[i]).append("\"");
						if ((i + 1) != size) {
							builder.append(seperator);
						}
					}
					// write the header to the file
					bufferedWriter.write(builder.append("\n").toString());
				}
			}

			// now loop through all the data
			while (it.hasNext()) {
				IHeadersDataRow row = it.next();
				// generate the data row
				Object[] dataRow = row.getValues();
				builder = new StringBuilder();
				i = 0;
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							builder.append("\"\"");
						} else {
							String thisStringVal = dataRow[i] + "";
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							builder.append("\"").append(thisStringVal).append("\"");
						}
					} else {
						// print out null
						if(dataRow[i] == null) {
							builder.append("null");
						} else {
							builder.append(dataRow[i]);
						}
					}
					// add sep between columns
					if ((i + 1) != size) {
						builder.append(seperator);
					}
				}
				// write row to file
				bufferedWriter.write(builder.append("\n").toString());
			}

		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (osw != null) {
					osw.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (fos != null) {
					fos.close();
				}
			} catch (IOException ioe) {
				logger.error(Constants.STACKTRACE, ioe);
			}
		}

		long end = System.currentTimeMillis();
		logger.debug("Time to output file = " + (end - start) + " ms");

		return f;
	}

	public static String encodeURIComponent(String s) {
		try {
			s = URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20").replaceAll("\\%21", "!").replaceAll("\\%27", "'")
					.replaceAll("\\%28", "(").replaceAll("\\%29", ")").replaceAll("\\%7E", "~");
		} catch (UnsupportedEncodingException uee) {
			logger.error(Constants.STACKTRACE, uee);
		}
		return s;
	}

	public static String decodeURIComponent(String s) {
		try {
			String newS = s.replaceAll("\\%20", "+").replace("!", "%21").replace("'", "%27")
					.replaceAll("\\(", "%28").replaceAll("\\)", "%29").replace("~", "%7E");
			s = URLDecoder.decode(newS, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return s;
	}

	// ensure no CRLF injection into logs for forging records
	public static String cleanLogString(String message) {
		if(message == null) {
			return message;
		}
		message = message.replace('\n', '_').replace('\r', '_').replace('\t', '_');

		if(DIHelper.getInstance().coreProp != null && Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.LOG_ENCODING) + "")) {
			message = ESAPI.encoder().encodeForHTML(message);
		}

		return message;
	}

	public static Map<String, String> cleanLogMap(Map<String, String> paramTable, String typeOfMap) {
		Map<String, String> cleanedParams = null;

		switch (typeOfMap.toUpperCase(Locale.ENGLISH)) {
			case "HASHTABLE":
				cleanedParams = new Hashtable<>();
				break;
			case "HASHMAP":
				cleanedParams = new HashMap<>();
				break;
			case "LINKEDHASHMAP":
				cleanedParams = new LinkedHashMap<>();
				break;
			case "TREEMAP":
				cleanedParams = new TreeMap<>();
				break;
			default:
				cleanedParams = new HashMap<>();
				break;
		}

		for (Entry<String, String> map: paramTable.entrySet()) {
			String cleanedKey = Utility.cleanLogString(map.getKey());
			String cleanedValue = Utility.cleanLogString(map.getValue());

			cleanedParams.put(cleanedKey, cleanedValue);
		}

		return cleanedParams;
	}

	// ensure no CRLF injection into responses for malicious attacks
	public static String cleanHttpResponse(String message) {
		if(message == null) {
			return message;
		}
		message = message
				.replace('\n', '_')
				.replace("%0d", "_")
				.replace('\r', '_')
				.replace("%0a", "_")
				.replace('\t', '_')
				.replace("%09", "_");

		message = Encode.forHtml(message);
		return message;
	}

	public static String normalizePath(String stringToNormalize) {
		if(stringToNormalize == null ) {
			return stringToNormalize;
		}
		String normalizedString = FilenameUtils.normalize(stringToNormalize);

		if (normalizedString == null) {
			logger.error("File path: " + Utility.cleanLogString(stringToNormalize) + " could not be normalized");
			throw new IllegalArgumentException();
		}
		normalizedString = normalizedString.replace("\\", "/");

		return normalizedString;
	}

	/**
	 * Loads the properties from a specified properties file.
	 * 
	 * @param filePath String of the name of the properties file to be loaded.
	 * @return Properties The properties imported from the prop file.
	 */
	public static Properties loadProperties(String filePath) {
		Properties retProp = new Properties();
		FileInputStream fis = null;
		if (filePath != null) {
			try {
				fis = new FileInputStream(Utility.normalizePath(filePath));
				retProp.load(fis);
			} catch (IOException ioe) {
				logger.info("Unable to read properties file: " + filePath);
				logger.error(Constants.STACKTRACE, ioe);
			} finally {
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException ioe) {
						logger.error(Constants.STACKTRACE, ioe);
					}
				}
			}
		}
		return retProp;
	}
	
	/**
	 * Determine if on the application we should cahce insights or not
	 * @return
	 */
	public static boolean getApplicationCacheInsight() {
		String cacheSetting = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHT_CACHEABLE);
		if(cacheSetting == null) {
			// default cache is true
			return true;
		}
		
		return Boolean.parseBoolean(cacheSetting);
	}
	
	/**
	 * Get the application time zone
	 * @return
	 */
	public static String getApplicationTimeZoneId() {
		String timeZone = DIHelper.getInstance().getProperty(Constants.DEFAULT_TIME_ZONE);
		if(timeZone == null || timeZone.trim().isEmpty()) {
			// default cache is true
			return "EST";
		}
		
		return timeZone.trim();
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
		} catch (MalformedURLException mue) {
			logger.error(Constants.STACKTRACE, mue);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		}
	}

	public static Map loadReactors(String folder, String key) {
		HashMap thisMap = new HashMap<String, Class>();
		try {
			// I should create the class pool everytime
			// this way it doesn't keep others and try to get from other places
			// does this end up loading all the other classes too ?
			ClassPool pool = ClassPool.getDefault();
			// takes a class and modifies the name of the package and then plugs it into the
			// heap

			// the main folder to add here is
			// basefolder/db/insightfolder/classes - right now I have it as classes. we can
			// change it to something else if we want
			String classesFolder = folder + "/classes";
			classesFolder = classesFolder.replaceAll("\\\\", "/");

			File file = new File(classesFolder);
			if (file.exists()) {
				// loads a class and tried to change the package of the class on the fly
				// CtClass clazz = pool.get("prerna.test.CPTest");

				logger.error("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph()
						// .whitelistPackages("prerna")
						.overrideClasspath((new File(classesFolder).toURI().toURL()))
						// .enableAllInfo()
						// .enableClassInfo()
						.whitelistPackages(packages).scan();
				// ScanResult sr = new ClassGraph().whitelistPackages("prerna").scan();
				// ScanResult sr = new
				// ClassGraph().enableClassInfo().whitelistPackages("prerna").whitelistPaths("C:/Users/pkapaleeswaran/workspacej3/MonolithDev3/target/classes").scan();

				// ClassInfoList classes =
				// sr.getAllClasses();//sr.getClassesImplementing("prerna.sablecc2.reactor.IReactor");
				
				ClassInfoList classes = sr.getSubclasses("prerna.sablecc2.reactor.AbstractReactor");

				Map<String, Class> reactors = new HashMap<>();
				// add the path to the insight classes so only this guy can load it
				pool.insertClassPath(classesFolder);

				for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
					String name = classes.get(classIndex).getSimpleName();
					String packageName = classes.get(classIndex).getPackageName();
					// Class actualClass = classes.get(classIndex).loadClass();
					// if it is already there.. nothing we can do
					if (!reactors.containsKey(name.toUpperCase().replaceAll("REACTOR", ""))) {
						try {
							// can I modify the class here
							CtClass clazz = pool.get(packageName + "." + name);
							clazz.defrost();
							String qClassName = key + "." + packageName + "." + name;
							// change the name of the classes
							// ideally we would just have the pakcage name change to the insight
							// this is to namespace it appropriately to have no issues
							// if you want a namespace
							clazz.setName(qClassName);
							Class newClass = clazz.toClass();

							Object newInstance = newClass.newInstance();

							// add to the insight map
							// we could do other instrumentation if we so chose to
							// once I have created it is in the heap, I dont need to do much. One thing I
							// could do is not load every class in the insight but give it out slowly
							if (newInstance instanceof prerna.sablecc2.reactor.AbstractReactor)
								thisMap.put(name.toUpperCase().replaceAll("REACTOR", ""), newClass);

						} catch (NotFoundException nfe) {
							logger.error(Constants.STACKTRACE, nfe);
						} catch (CannotCompileException cce) {
							logger.error(Constants.STACKTRACE, cce);
						}

						// once the new instance has been done.. it has been injected into heap.. after
						// this anyone can access it.
						// no way to remove this class from heap
						// has to be garbage collected as it moves
					}
				}
			}
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		}

		return thisMap;
	}

	public static Map loadReactors(String folder, String key, SemossClassloader cl) {
		return loadReactors(folder, key, cl, "classes");
	}

	// loads classes through this specific class loader for the insight
	public static Map loadReactors(String folder, String key, SemossClassloader cl, String outputFolder) {
		HashMap thisMap = new HashMap<String, Class>();
		try {
			// I should create the class pool everytime
			// this way it doesn't keep others and try to get from other places
			// does this end up loading all the other classes too ?
			ClassPool pool = ClassPool.getDefault();
			// takes a class and modifies the name of the package and then plugs it into the
			// heap

			// the main folder to add here is
			// basefolder/db/insightfolder/classes - right now I have it as classes. we can
			// change it to something else if we want
			String classesFolder = folder + "/" + outputFolder;

			classesFolder = classesFolder.replaceAll("\\\\", "/");
			cl.folder = classesFolder;

			File file = new File(classesFolder);
			if (file.exists()) {
				// loads a class and tried to change the package of the class on the fly
				// CtClass clazz = pool.get("prerna.test.CPTest");

				logger.error("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph()
						// .whitelistPackages("prerna")
						.overrideClasspath((new File(classesFolder).toURI().toURL()))
						// .enableAllInfo()
						// .enableClassInfo()
						.whitelistPackages(packages).scan();
				// ScanResult sr = new ClassGraph().whitelistPackages("prerna").scan();
				// ScanResult sr = new
				// ClassGraph().enableClassInfo().whitelistPackages("prerna").whitelistPaths("C:/Users/pkapaleeswaran/workspacej3/MonolithDev3/target/classes").scan();

				// ClassInfoList classes =
				// sr.getAllClasses();//sr.getClassesImplementing("prerna.sablecc2.reactor.IReactor");
				ClassInfoList classes = sr.getSubclasses("prerna.sablecc2.reactor.AbstractReactor");

				Map<String, Class> reactors = new HashMap<>();
				// add the path to the insight classes so only this guy can load it
				pool.insertClassPath(classesFolder);

				for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
					// this will load the whole thing
					Class newClass = cl.loadClass(classes.get(classIndex).getName());
					String name = classes.get(classIndex).getSimpleName();

					thisMap.put(name.toUpperCase().replaceAll("REACTOR", ""), newClass);
				}
			}
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		}

		return thisMap;
	}

	public static String getCP() {
		String envClassPath = null;

		try {
			StringBuilder retClassPath = new StringBuilder("");
			Class utilClass = Class.forName("prerna.util.Utility");
			ClassLoader cl = utilClass.getClassLoader();

			URL[] urls = ((URLClassLoader) cl).getURLs();

			for (URL url : urls) {
				String thisURL = URLDecoder.decode((url.getFile().replaceFirst("/", "")));
				if (thisURL.endsWith("/"))
					thisURL = thisURL.substring(0, thisURL.length() - 1);

				retClassPath
						// .append("\"")
						.append(thisURL)
						// .append("\"")
						.append(";");

			}
			envClassPath = "\"" + retClassPath.toString() + "\"";
		} catch (ClassNotFoundException cnfe) {
			logger.error(Constants.STACKTRACE, cnfe);
		}

		return envClassPath;
	}

	public static String getCP(String specificJars, String insightFolder) {
		StringBuffer envClassPath = new StringBuffer();
		String osName = System.getProperty("os.name").toLowerCase();
		boolean win = osName.indexOf("win") >= 0;

		try {
			StringBuffer retClassPath = new StringBuffer("");
			Class utilClass = Class.forName("prerna.util.Utility");
			ClassLoader cl = utilClass.getClassLoader();

			URL[] urls = ((URLClassLoader) cl).getURLs();

			String webinfLib = null;
			boolean webinfTagged = false;

			for (URL url : urls) {

				String jarName = Utility.getInstanceName(url + "");
				// jarName = jarName.replace(".jar", "");
				String thisURL = URLDecoder.decode((url.getFile().replaceFirst("/", "")));

				String separator = ";";
				if (!win) {
					thisURL = "/" + thisURL;
					separator = ":";
				}

				if (thisURL.endsWith(".jar") && thisURL.contains("WEB-INF/lib") && webinfLib == null) {
					String thisJarName = getInstanceName(thisURL);
					thisURL = thisURL.replace("/" + thisJarName, "");
					webinfLib = thisURL + "/*";
					if (!webinfTagged) {
						retClassPath
								// .append("\"")
								.append(webinfLib)
								// .append("\"")
								.append(separator);
						webinfTagged = true;
					}

				}

				// add the folder
				else if (!thisURL.endsWith(".jar") && specificJars.contains(jarName)
						&& !thisURL.contains("WEB-INF/lib")) {
					if (thisURL.endsWith("/"))
						thisURL = thisURL.substring(0, thisURL.length() - 1);
					retClassPath
							// .append("\"")
							.append(thisURL)
							// .append("\"")
							.append(separator);
				}
				// address the issue when you are running outside of semoss
				else if (thisURL.endsWith(".jar") && specificJars.contains(jarName)
						&& !thisURL.contains("WEB-INF/lib")) {
					if (thisURL.endsWith("/"))
						thisURL = thisURL.substring(0, thisURL.length() - 1);
					retClassPath
							// .append("\"")
							.append(thisURL)
							// .append("\"")
							.append(separator);
				}

			}
			// remove the last one
			String cp = retClassPath.toString();
			String curPath = insightFolder + ";";
			if(!win)
				curPath = insightFolder + ":";

			envClassPath = new StringBuffer("\"" + curPath + cp.substring(0, cp.length() - 1) + "\"");
		} catch (ClassNotFoundException cnfe) {
			logger.error(Constants.STACKTRACE, cnfe);
		}

		return envClassPath.toString();
	}

	public static Process startTCPServer(String cp, String insightFolder, String port) {
		// this basically starts a java process
		// the string is an identifier for this process
		Process thisProcess = null;
		if (cp == null) {
			cp = "fst-2.56.jar;jep-3.9.0.jar;log4j-1.2.17.jar;commons-io-2.4.jar;objenesis-2.5.1.jar;jackson-core-2.9.5.jar;javassist-3.20.0-GA.jar;netty-all-4.1.47.Final.jar;classes";
		}
		String specificPath = getCP(cp, insightFolder);
		try {
			String java = System.getenv("JAVA_HOME");
			if (java == null) {
				java = DIHelper.getInstance().getProperty("JAVA_HOME");
			}
			if(!java.endsWith("bin")) //seems like for graal
				java = java + "/bin/java";
			else
				java = java + "/java";
			// account for spaces in the path to java
			if (java.contains(" ")) {
				java = "\"" + java + "\"";
			}
			// change the \\
			java = java.replace("\\", "/");

			String jep = DIHelper.getInstance().getProperty("LD_LIBRARY_PATH");
			if (jep == null) {
				jep = System.getenv("LD_LIBRARY_PATH");
			}
			// account for spaces in the path to jep
			if (jep.contains(" ")) {
				jep = "\"" + jep + "\"";
			}
			jep = jep.replace("\\", "/");

			String pyWorker = DIHelper.getInstance().getProperty("TCP_WORKER");
			if(pyWorker == null)
				pyWorker = "prerna.tcp.Server";
			String[] commands = null;
			if (port == null)
				commands = new String[7];
			else {
				commands = new String[8];
				commands[7] = port;
			}
			String finalDir = insightFolder.replace("\\", "/");
			commands[0] = java;
			// just append all the environment variables
			// on the windows machine as well
			if(SystemUtils.IS_OS_WINDOWS) {
				// since we will wrap quotes around the entire thing as PATH likely has spaces
				// remove from jep
				if(jep.startsWith("\"") && jep.endsWith("\"")) {
					jep = jep.substring(1, jep.length()-1);
				}
				commands[1] = "-Djava.library.path=\"%PATH%;" + jep + "\"";
			} else {
				commands[1] = "-Djava.library.path=" + jep;
			}
			commands[2] = "-cp";
			commands[3] = specificPath;
			commands[4] = pyWorker;
			commands[5] = finalDir;
			commands[6] = DIHelper.getInstance().rdfMapFileLocation;
			// java = "c:/zulu/zulu-8/bin/java";
			// StringBuilder argList = new StringBuilder(args[0]);
			// for(int argIndex = 0;argIndex < args.length;argList.append("
			// ").append(args[argIndex]), argIndex++);
			// commands[2] = "-Dlog4j.configuration=" + finalDir + "/log4j.properties";
			/*commands[3] = "C:/Users/pkapaleeswaran/.m2/repository/de/ruedigermoeller/fst/2.56/fst-2.56.jar;"
					+ "C:/Python/Python36/Lib/site-packages/jep/jep-3.9.0.jar;"
					+ "c:/users/pkapaleeswaran/workspacej3/semossdev/target/classes;"
					+ "C:/Users/pkapaleeswaran/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar;"
					+ "C:/Users/pkapaleeswaran/.m2/repository/commons-io/commons-io/2.2/commons-io-2.2.jar;";
			*/
			// commands[5] = "c:/users/pkapaleeswaran/workspacej3/temp/filebuffer";
			// commands[6] = ">";
			// commands[7] = finalDir + "/.log";

			logger.debug("Trying to create file in .. " + finalDir);
			File file = new File(finalDir + "/init");
			file.createNewFile();
			logger.debug("Python start commands ... ");
			logger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// run it as a process
			// ProcessBuilder pb = new ProcessBuilder(commands);
			// ProcessBuilder pb = new
			// ProcessBuilder("c:/users/pkapaleeswaran/workspacej3/temp/mango.bat");
			// pb.command(commands);

			

			if(!(Strings.isNullOrEmpty(DIHelper.getInstance().getProperty("ULIMIT_R_MEM_LIMIT")))){
				String ulimit = DIHelper.getInstance().getProperty("ULIMIT_R_MEM_LIMIT");
			StringBuilder sb = new StringBuilder();
			for (String str : commands) {
				sb.append(str).append(" ");
			}
			sb.substring(0, sb.length() - 1);
			commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " +  ulimit + " && " + sb.toString() + "\"" };
			}

			
			String[] starterFile = writeStarterFile(commands, finalDir);
			ProcessBuilder pb = new ProcessBuilder(starterFile);
			pb.redirectError();
			logger.info("came out of the waiting for process");
			Process p = pb.start();

			try {
				// p.waitFor();
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				logger.error(Constants.STACKTRACE, ie);
			}
			logger.info("came out of the waiting for process");
			thisProcess = p;

			// System.out.println("Process started with .. " + p.exitValue());
			// thisProcess = Runtime.getRuntime().exec(java + " -cp " + cp + " " + className
			// + " " + argList);
			// thisProcess = Runtime.getRuntime().exec(java + " " + className + " " +
			// argList + " > c:/users/pkapaleeswaran/workspacej3/temp/java.run");
			// thisProcess = pb.start();
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		}

		return thisProcess;
	}

	public static String[] writeStarterFile(String[] commands, String dir) {
		// check if the os is unix and if so make it .sh
		String osName = System.getProperty("os.name").toLowerCase();


		String starter = ""; 
		String[] commandsStarter = null;

		if (osName.indexOf("win") >= 0) {
			commandsStarter = new String[1];
			commandsStarter[0] = dir + "/starter.bat";
			starter = dir + "/starter.bat";
		}
		if (osName.indexOf("win") < 0) {
			commandsStarter =  new String[2];
			commandsStarter[0] = "/bin/bash";
			starter = dir + "/starter.sh";
			commandsStarter[1] = starter;
		}
		try {
			File starterFile = new File(starter);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			for (int cmdIndex = 0; cmdIndex < commands.length; cmdIndex++) {
				baos.write(commands[cmdIndex].getBytes());
				baos.write("  ".getBytes());
			}
			FileUtils.writeByteArrayToFile(starterFile, baos.toByteArray());

			// chmod in case.. who knows
			if (osName.indexOf("win") < 0) {
				ProcessBuilder p = new ProcessBuilder("/bin/chmod", "777", starter);
				p.start();
			}
		} catch (FileNotFoundException fnfe) {
			logger.error(Constants.STACKTRACE, fnfe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		}

		return commandsStarter;
	}

	// compiler methods
	public static int compileJava(String folder, String classpath) {
		// TODO Auto-generated method stub
		com.sun.tools.javac.Main javac = new com.sun.tools.javac.Main();
		/*		String[] args2 = new String[] {
				        "-d", "c:/users/pkapaleeswaran/workspacej3/SemossDev",
				        "c:/users/pkapaleeswaran/workspacej3/SemossDev/independent/HelloReactor.java"
				        , "-proc:none"
				    };
		*/
		// do I have to compile individually
		String javaFolder = folder + "/java";

		File file = new File(javaFolder);
		int status = -1;

		// one last piece of optimization I need to perform is check timestamp before
		// compiling
		if (file.exists() && file.isDirectory()) {
			logger.info("Compiling Java in Folder " + javaFolder);
			List<String> files = GitAssetUtils.listAssets(javaFolder, "*.java", null, null, null);
			String outputFolder = folder + "/classes";
			File outDir = new File(outputFolder);
			if (!outDir.exists())
				outDir.mkdir();

			if (files.size() > 0) {
				String[] compiler = new String[files.size() + 5];
				compiler[0] = "-d";
				compiler[1] = outputFolder;
				compiler[2] = "-cp";
				compiler[3] = classpath;
				compiler[4] = "-proc:none";

				for (int fileIndex = 0; fileIndex < files.size(); fileIndex++)
					compiler[fileIndex + 5] = files.get(fileIndex);

				/*
				// https://stackoverflow.com/questions/43768021/how-to-store-the-result-of-compilation-using-javac-to-a-text-file
				// when we use process builder
				
				compiler[files.size() + 5] = "2>";
				compiler[files.size() + 6] = folder + "/classes/compileerrors.out";
				*/
				try {
					java.io.PrintWriter pw = new java.io.PrintWriter(new File(folder + "/classes/compileerror.out"));
					
					status = javac.compile(compiler, pw);
					pw.close();
				} catch (FileNotFoundException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				status = javac.compile(compiler);
			}

			/*for(int fileIndex = 0;fileIndex < files.size();fileIndex++)
			{
				String inputFile = files.get(fileIndex);
				// so need a way to set the classpath
				//envClassPath = null;
				String[] args2 = new String[] {
				        "-d", outputFolder ,
				        "-cp", classpath,
				        inputFile
				        , "-proc:none"
				    };
			
				    int status = javac.compile(args2);
			}*/

		}
		return status;
	}

	public static int findOpenPort2() {

		int retPort = -1;

		int lowPort = 1024;
		int highPort = 6666;

		if (DIHelper.getInstance().getProperty("LOW_PORT") != null)
			lowPort = Integer.parseInt(DIHelper.getInstance().getProperty("LOW_PORT"));

		if (DIHelper.getInstance().getProperty("HIGH_PORT") != null)
			highPort = Integer.parseInt(DIHelper.getInstance().getProperty("HIGH_PORT"));

		for (int port = lowPort; port < highPort; port++) {

			logger.debug("Trying to see if port " + port + " is open for Rserve.");
			try {
				ServerSocket s = new ServerSocket(port);
				s.close();
				logger.debug("Success! Port: " + port);
				retPort = port;
				break;
			} catch (Exception ex) {
				// Port isn't open, notify and move on
				logger.error("Port " + port + " is unavailable.");
			}
		}
		return retPort;
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
//			logger.info(engine.getEngineName() + " does not have an insight rdbms");
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
//				logger.error("COULD NOT FIND INSIGHTS QUESTION_ID TABLE FOR ENGINE = " + engine.getEngineName());
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
//			logger.info(engineName + " does not have an insight rdbms");
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
//			logger.info(engineName + " does not have explore an instance query in database to update");
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

	public static String getEngineData(String queryEngine)
	{
		if(engineIdMap.size() == 0)
		{
			List <Map<String, Object>> allEngines = SecurityQueryUtils.getAllDatabaseList();
	
			for(int engineIndex = 0;engineIndex < allEngines.size();engineIndex++)
			{
				Map <String, Object> engineValues = allEngines.get(engineIndex);
				String engineName = (String)engineValues.get("app_name");
				String engineId = (String)engineValues.get("app_id");
				
			
				engineIdMap.put(engineName, engineId);
			}
		}
		return engineIdMap.get(queryEngine);

	}

	public static void main(String[] args) {
		DIHelper.getInstance().loadCoreProp("c:/users/pkapaleeswaran/workspacej3/MonolithDev5/RDF_Map_web.prop");
		Utility.startTCPServer(null, "c:/users/pkapaleeswaran/workspacej3/temp/filebuffer", "6666");
	}

}
