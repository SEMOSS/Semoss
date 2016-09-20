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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
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
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.h2.jdbc.JdbcClob;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;

import prerna.algorithm.api.IMetaData;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.om.SEMOSSParam;
import prerna.poi.main.BaseDatabaseCreator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.solr.SolrIndexEngine;
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

	/**
	 * Add the engine instances into the solr index engine
	 * @param engineToAdd					The IEngine to add into the solr index engine
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws ParseException
	 */
	public static void addToSolrInstanceCore(IEngine engineToAdd) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, ParseException{
		// get the engine name
		String engineName = engineToAdd.getEngineName();
		// get the solr index engine
		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
		// if the solr is active...
		if (solrE.serverActive()) {

			List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			/*
			 * The unique document is the engineName concatenated with the concept
			 * BUT for properties it is the engineName concatenated with the concept concatenated with the property 
			 * Note: we only add properties that are not numeric
			 * 
			 * Logic is as follows
			 * 1) Get the list of all the concepts
			 * 2) For each concept add the concept with all its instance values to the docs list
			 * 3) If the concept has properties, perform steps 4 & 5
			 * 		4) for each property, get the list of values
			 * 		5) if property is categorical, add the property with all its instance values as a document to the docs list
			 * 6) Index all the documents that are stored in docs
			 * 
			 * There is a very annoying caveat.. we have an annoying bifurcation based on the engine type
			 * If it is a RDBMS, getting the properties is pretty easy based on the way the IEngine is set up and how RDBMS queries work
			 * However, for RDF, getting the properties requires us to create a query and execute that query to get the list of values :/
			 */

			//TODO: WE NOW STORE THE DATA TYPES ON THE OWL!!! NO NEED TO PERFORM THE QUERY BEFORE CHECKING THE TYPE!!!!!
			//TODO: will come back to this

			// 1) grab all concepts that exist in the database
			List<String> conceptList = engineToAdd.getConcepts2(false);
			for(String concept : conceptList) {
				// we ignore the default concept node...
				if(concept.equals("http://semoss.org/ontologies/Concept")) {
					continue;
				}

				// 2) get all the instances for the concept
				// fieldData will store the instance document information when we add the concept
				List<Object> instances = engineToAdd.getEntityOfType(concept);
				if(instances.isEmpty()) {
					// sometimes this list is empty when users create databases with empty fields that are
					// meant to filled in via forms 
					continue;
				}
				// create the concept unique id which is the engineName concatenated with the concept
				String newId = engineName + "_" + concept;

				//use the method that you just made to save the concept
				Map<String, Object> fieldData = new HashMap<String, Object>();
				fieldData.put(SolrIndexEngine.CORE_ENGINE, engineName);
				fieldData.put(SolrIndexEngine.VALUE, concept);
				// sadly, the instances we get back are URIs.. need to extract the instance values from it
				List<Object> instancesList = new ArrayList<Object>();
				for(Object instance : instances) {
					instancesList.add(Utility.getInstanceName(instance + ""));
				}
				fieldData.put(SolrIndexEngine.INSTANCES, instancesList);
				// add to the docs list
				docs.add(solrE.createDocument(newId, fieldData));

				// 3) now see if the concept has properties
				List<String> propName = engineToAdd.getProperties4Concept2(concept, false);
				if(propName.isEmpty()) {
					// if no properties, go onto the next concept
					continue;
				}

				// we found properties, lets try to add those as well
				// here we have the bifurcation based on the engine type
				if(engineToAdd.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)) {
					NEXT_PROP : for(String prop : propName) {
						Vector<Object> propertiesList = engineToAdd.getEntityOfType(prop);
						boolean isNumeric = false;
						Iterator<Object> it = propertiesList.iterator();
						while(it.hasNext()) {
							Object o = it.next();
							if(o == null || o.toString().isEmpty()) {
								it.remove();
								continue;
							}
							if(o instanceof Number || isStringDate(o + "")) {
								isNumeric = true;
								continue NEXT_PROP;
							}
						}

						// add if the property and its instances to the docs list
						if(!isNumeric && !propertiesList.isEmpty()) {
							String propId = newId + "_" + prop; // in case there are properties for the same engine, tie it to the concept
							Map<String, Object> propFieldData = new HashMap<String, Object>();
							propFieldData.put(SolrIndexEngine.CORE_ENGINE, engineName);
							propFieldData.put(SolrIndexEngine.VALUE, prop);
							propertiesList.add(Utility.getInstanceName(prop));
							propFieldData.put(SolrIndexEngine.INSTANCES, propertiesList);
							// add the property document to the docs
							docs.add(solrE.createDocument(propId, propFieldData));
						}
					}
				} else {
					for(String prop : propName) {
						// there is no way to get the list of properties for a specific concept in RDF through the interface
						// create a query using the concept and the property name
						String propQuery = "SELECT DISTINCT ?property WHERE { {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + concept + "> } { ?x <" + prop + "> ?property} }";
						ISelectWrapper propWrapper = WrapperManager.getInstance().getSWrapper(engineToAdd, propQuery);
						List<Object> propertiesList = new ArrayList<Object>();
						while (propWrapper.hasNext()) {
							ISelectStatement propSS = propWrapper.next();
							Object property = propSS.getVar("property");
							if(property instanceof String && !isStringDate((String)property)){
								//replace property underscores with space
								property = property.toString();
								propertiesList.add(property);
							}
						}

						// add if the property and its instances to the docs list
						if(!propertiesList.isEmpty()) {
							String propId = newId + "_" + prop; // in case there are properties for the same engine, tie it to the concept
							Map<String, Object> propFieldData = new HashMap<String, Object>();
							propFieldData.put(SolrIndexEngine.CORE_ENGINE, engineName);
							propFieldData.put(SolrIndexEngine.VALUE, prop);
							propertiesList.add(Utility.getInstanceName(prop));
							propFieldData.put(SolrIndexEngine.INSTANCES, propertiesList);
							// add the property document to the docs
							docs.add(solrE.createDocument(propId, propFieldData));
						}
					}
				}
			}

			// 6) index all the documents at the same time for efficiency
			try {
				solrE.addInstances(docs);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Add the engine insights into the solr index engine
	 * @param engineToAdd					The IEngine to add into the solr index engine
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 */
	public static void addToSolrInsightCore(IEngine engineToAdd) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		//		// get the engine name
		String engineName = engineToAdd.getEngineName();
		// get the solr index engine
		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
		// if the solr is active...
		if (solrE.serverActive()) {

			List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			/*
			 * The unique document is the engineName concatenated with the engine unique rdbms name (which is just a number)
			 * 
			 * Logic is as follows
			 * 1) Delete all existing insights that are tagged by this engine 
			 * 2) Execute a query to get all relevant information from the engine rdbms insights database
			 * 3) For each insight, grab the relevant information and store into a solr document and add it to the docs list
			 * 4) Index all the documents stored in docs list
			 */

			// 1) delete any existing insights from this engine
			solrE.deleteEngine(engineName);

			// also going to get some default field values since they are not captured anywhere...

			// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
			DateFormat dateFormat = SolrIndexEngine.getDateFormat();
			Date date = new Date();
			String currDate = dateFormat.format(date);
			// set all the users to be default...
			String userID = "default";


			// 2) execute the query and iterate through the insights
			String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_DATA_MAKER FROM QUESTION_ID";
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engineToAdd.getInsightDatabase(), query);
			while(wrapper.hasNext()){
				ISelectStatement ss = wrapper.next();

				// 3) start to get all the relevant metadata surrounding the insight

				// get the unique id of the insight within the engine
				int id = (int) ss.getVar("ID");
				// get the question name
				String name = (String) ss.getVar("QUESTION_NAME");
				// get the question layout
				String layout = (String) ss.getVar("QUESTION_LAYOUT");
				// get the data maker name
				String dataMakerName = (String) ss.getVar("QUESTION_DATA_MAKER");

				// get the question perspective to use as a default tag
				String perspective = (String) ss.getVar("QUESTION_PERSPECTIVE");
				// sadly, at some point the perspective which we use as a tag has been added 
				// using the following 3 ways...
				// remove all 3 if found
				String perspString1 ="-Perspective";
				String perspString2 ="Perspective";
				String perspString3 ="_Perspective";
				if (perspective.contains(perspString1)) {
					perspective = perspective.replace(perspString1, "").trim();
				}
				if(perspective.contains(perspString2)){
					perspective = perspective.replace(perspString2, "").trim();
				}
				if(perspective.contains(perspString3)){
					perspective = perspective.replace(perspString3, "").trim();
				}

				// get the clob containing the question makeup
				// TODO: we use this to query it and get the list of engines associated with an insight
				// TODO: however, since the DMC is no longer valid and that logic is placed within the PKQL
				// TODO: we need to not do this and figure out a way to get the engines that are used in the PKQL

				/////// START CLOB PROCESSING TO GET LIST OF ENGINES ///////
				JdbcClob obj = (JdbcClob) ss.getVar("QUESTION_MAKEUP"); 
				InputStream makeup = null;
				try {
					makeup = obj.getAsciiStream();
				} catch (SQLException e) {
					e.printStackTrace();
				}

				//load the makeup input stream into a rc
				RepositoryConnection rc = null;
				try {
					Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
					myRepository.initialize();
					rc = myRepository.getConnection();
					rc.add(makeup, "semoss.org", RDFFormat.NTRIPLES);
				} catch (RuntimeException ignored) {
					ignored.printStackTrace();
				} catch (RDFParseException e) {
					e.printStackTrace();
				} catch (RepositoryException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				// set the rc in the in-memory engine
				InMemorySesameEngine myEng = new InMemorySesameEngine();
				myEng.setRepositoryConnection(rc);

				Set<String> engineSet = new HashSet<String>();
				// query the in-memory sesame engine to get the list of engines
				String engineQuery = "SELECT DISTINCT ?EngineName WHERE {{?Engine a <http://semoss.org/ontologies/Concept/Engine>}{?Engine <http://semoss.org/ontologies/Relation/Contains/Name> ?EngineName} }";
				ISelectWrapper engineWrapper = WrapperManager.getInstance().getSWrapper(myEng, engineQuery);
				while(engineWrapper.hasNext()) {
					ISelectStatement engineSS = engineWrapper.next();
					engineSet.add(engineSS.getVar("EngineName") + "");
				}
				// since pkql adds only one dmc with engine being local master... we want to remove that
				// and set engine into the set
				engineSet.remove(Constants.LOCAL_MASTER_DB_NAME);
				engineSet.add(engineName);
				/////// END CLOB PROCESSING TO GET LIST OF ENGINES ///////

				// get the list of params associated with the insight and add that into the paramList
				// TODO: this will also become out dated just like the list of engines logic above when it is shifted into PKQL
				List<String> paramList = new ArrayList<String>();
				List<SEMOSSParam> params = engineToAdd.getParams(id + "");
				if(params != null && !params.isEmpty()) {
					for(SEMOSSParam p : params) {
						paramList.add(p.getName());
					}
				}

				// have all the relevant fields now, so store with appropriate schema name
				// create solr document and add into docs list
				Map<String, Object>  queryResults = new  HashMap<> ();
				queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
				queryResults.put(SolrIndexEngine.INDEX_NAME, name);
				queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
				queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
				queryResults.put(SolrIndexEngine.USER_ID, userID);
				queryResults.put(SolrIndexEngine.ENGINES, engineSet);
				queryResults.put(SolrIndexEngine.PARAMS, paramList);
				queryResults.put(SolrIndexEngine.CORE_ENGINE, engineName);
				queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, id);
				queryResults.put(SolrIndexEngine.LAYOUT, layout);
				queryResults.put(SolrIndexEngine.TAGS, perspective);
				queryResults.put(SolrIndexEngine.DATAMAKER_NAME, dataMakerName);
				try {
					docs.add(solrE.createDocument(engineName + "_" + id, queryResults));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// 4) index all the documents at the same time for efficiency
			try {
				solrE.addInsights(docs);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	public static void addToSolrInsightCore2(String engineName) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		
		//		// get the engine name		
		// generate the appropriate query to execute on the local master engine to get the time stamp
		System.out.println("SOLR'ing on " + engineName);
		
		String engineFile = DIHelper.getInstance().getCoreProp().getProperty(engineName + "_" + Constants.STORE);
		
		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
		Properties prop = new Properties();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		FileInputStream fis = null;
		String engineDbTime = null;
		String dbLocation = null;
		
		try {
			fis = new FileInputStream(engineFile);
			prop.load(fis);
		
			// find when the database was last modified to see the time
			dbLocation = (String)prop.get(Constants.RDBMS_INSIGHTS);
			String dbFileLocation = baseFolder + "/" + dbLocation + ".mv.db";
			File dbfile = new File(dbFileLocation);
			
			BasicFileAttributes bfa = Files.readAttributes(dbfile.toPath(), BasicFileAttributes.class);
			FileTime ft = bfa.lastModifiedTime();
			DateFormat df = SolrIndexEngine.getDateFormat();
			engineDbTime = df.format(ft.toMillis());
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
		}
			}
		}

		// make the engine compare if this is valid
		if(!solrE.containsEngine(engineName))
		{
			
			// this has all the details
			// the engine file is primarily the SMSS that is going to be utilized for the purposes of retrieving all the data
			//jdbc:h2:@BaseFolder@/db/@ENGINE@/database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768
			String jdbcURL = "jdbc:h2:" + baseFolder + "/" + dbLocation + ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
			System.out.println("Connecting to URL.. " + jdbcURL);
			String userName = "sa";
			String password = "";
//			if(prop.containsKey(Constants.USERNAME))
//				userName = prop.getProperty(Constants.USERNAME);
//			if(prop.containsKey(Constants.PASSWORD))
//				password = prop.getProperty(Constants.PASSWORD);
			
			RDBMSNativeEngine rne = new RDBMSNativeEngine();
			rne.makeConnection(jdbcURL, userName, password);
			MetaHelper helper = new MetaHelper(null, null, null, rne);
	
			// get the solr index engine
			// if the solr is active...
			if (solrE.serverActive()) {
	
				List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
				/*
				 * The unique document is the engineName concatenated with the engine unique rdbms name (which is just a number)
				 * 
				 * Logic is as follows
				 * 1) Delete all existing insights that are tagged by this engine 
				 * 2) Execute a query to get all relevant information from the engine rdbms insights database
				 * 3) For each insight, grab the relevant information and store into a solr document and add it to the docs list
				 * 4) Index all the documents stored in docs list
				 */
				
				// 1) delete any existing insights from this engine
				solrE.deleteEngine(engineName);
				
				// also going to get some default field values since they are not captured anywhere...
				
				// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
				DateFormat dateFormat = SolrIndexEngine.getDateFormat();
				Date date = new Date();
				String currDate = engineDbTime;
				// set all the users to be default...
				String userID = "default";
				
				
				// 2) execute the query and iterate through the insights
				String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, QUESTION_DATA_MAKER FROM QUESTION_ID";
				ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rne, query);
				while(wrapper.hasNext()){
					ISelectStatement ss = wrapper.next();
					
					// 3) start to get all the relevant metadata surrounding the insight
					
					// get the unique id of the insight within the engine
					int id = (int) ss.getVar("ID");
					// get the question name
					String name = (String) ss.getVar("QUESTION_NAME");
					// get the question layout
					String layout = (String) ss.getVar("QUESTION_LAYOUT");
					// get the data maker name
					String dataMakerName = (String) ss.getVar("QUESTION_DATA_MAKER");
	
					// get the question perspective to use as a default tag
					String perspective = (String) ss.getVar("QUESTION_PERSPECTIVE");
					// sadly, at some point the perspective which we use as a tag has been added 
					// using the following 3 ways...
					// remove all 3 if found
					String perspString1 ="-Perspective";
					String perspString2 ="Perspective";
					String perspString3 ="_Perspective";
					if (perspective.contains(perspString1)) {
						perspective = perspective.replace(perspString1, "").trim();
					}
					if(perspective.contains(perspString2)){
						perspective = perspective.replace(perspString2, "").trim();
					}
					if(perspective.contains(perspString3)){
						perspective = perspective.replace(perspString3, "").trim();
					}
					
					// get the clob containing the question makeup
					// TODO: we use this to query it and get the list of engines associated with an insight
					// TODO: however, since the DMC is no longer valid and that logic is placed within the PKQL
					// TODO: we need to not do this and figure out a way to get the engines that are used in the PKQL
					
					/////// START CLOB PROCESSING TO GET LIST OF ENGINES ///////
					JdbcClob obj = (JdbcClob) ss.getVar("QUESTION_MAKEUP"); 
					InputStream makeup = null;
					try {
						makeup = obj.getAsciiStream();
					} catch (SQLException e) {
						e.printStackTrace();
					}
	
					//load the makeup input stream into a rc
					RepositoryConnection rc = null;
					try {
						Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
						myRepository.initialize();
						rc = myRepository.getConnection();
						rc.add(makeup, "semoss.org", RDFFormat.NTRIPLES);
					} catch (RuntimeException ignored) {
						ignored.printStackTrace();
					} catch (RDFParseException e) {
						e.printStackTrace();
					} catch (RepositoryException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					// set the rc in the in-memory engine
					InMemorySesameEngine myEng = new InMemorySesameEngine();
					myEng.setRepositoryConnection(rc);
	
					Set<String> engineSet = new HashSet<String>();
					// query the in-memory sesame engine to get the list of engines
					String engineQuery = "SELECT DISTINCT ?EngineName WHERE {{?Engine a <http://semoss.org/ontologies/Concept/Engine>}{?Engine <http://semoss.org/ontologies/Relation/Contains/Name> ?EngineName} }";
					ISelectWrapper engineWrapper = WrapperManager.getInstance().getSWrapper(myEng, engineQuery);
					while(engineWrapper.hasNext()) {
						ISelectStatement engineSS = engineWrapper.next();
						engineSet.add(engineSS.getVar("EngineName") + "");
					}
					// since pkql adds only one dmc with engine being local master... we want to remove that
					// and set engine into the set
					engineSet.remove(Constants.LOCAL_MASTER_DB_NAME);
					engineSet.add(engineName);
					/////// END CLOB PROCESSING TO GET LIST OF ENGINES ///////
	
					// get the list of params associated with the insight and add that into the paramList
					// TODO: this will also become out dated just like the list of engines logic above when it is shifted into PKQL
					List<String> paramList = new ArrayList<String>();
					List<SEMOSSParam> params = helper.getParams(id + "");
					if(params != null && !params.isEmpty()) {
						for(SEMOSSParam p : params) {
							paramList.add(p.getName());
						}
					}
	
					// have all the relevant fields now, so store with appropriate schema name
					// create solr document and add into docs list
					Map<String, Object>  queryResults = new  HashMap<> ();
					queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
					queryResults.put(SolrIndexEngine.INDEX_NAME, name);
					queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
					queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
					queryResults.put(SolrIndexEngine.USER_ID, userID);
					queryResults.put(SolrIndexEngine.ENGINES, engineSet);
					queryResults.put(SolrIndexEngine.PARAMS, paramList);
					queryResults.put(SolrIndexEngine.CORE_ENGINE, engineName);
					queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, id);
					queryResults.put(SolrIndexEngine.LAYOUT, layout);
					queryResults.put(SolrIndexEngine.TAGS, perspective);
					queryResults.put(SolrIndexEngine.DATAMAKER_NAME, dataMakerName);
					try {
						docs.add(solrE.createDocument(engineName + "_" + id, queryResults));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
	
				// 4) index all the documents at the same time for efficiency
				try {
					solrE.addInsights(docs);
					rne.closeDB();
					fis.close();
				} catch (SolrServerException | IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("Completed " + engineName);
		}
		else
		{
			System.out.println("Exists !!");
		}
	}

	//force solr to load once 
	//once engine is loaded, set boolean to false
	/**
	 * Changes a value within the SMSS file for a given key
	 * @param smssPath					The path to the SMSS file
	 * @param keyToAlter				The key to alter
	 * @param valueToProvide			The value to give the key
	 */
	public static void changeSMSSValue(String smssPath, String keyToAlter, String valueToProvide) {
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
				if(content.get(i).contains(keyToAlter)){
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
		FileOutputStream fileOut = null;
		File file = new File(smssPath);
		String locInFile = "OWL";

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
				if(content.get(i).contains(locInFile)){
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
	 * Delete all the insights surrounding a specified engine
	 * @param engineName				
	 */
	public static void deleteFromSolr(String engineName) {
		try {
			SolrIndexEngine.getInstance().deleteEngine(engineName);
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
			e.printStackTrace();
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
		cleaned = cleaned.replaceAll("\\,", "");
		cleaned = cleaned.replaceAll("\\%", "");
		cleaned = cleaned.replaceAll("\\-", "");
		cleaned = cleaned.replaceAll("\\(", "");
		cleaned = cleaned.replaceAll("\\)", "");
		cleaned = cleaned.replaceAll("\\&", "and");
		while(cleaned.contains("__")) {
			cleaned = cleaned.replace("__", "_");
		}

		return cleaned;
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
	public static Vector<String> getVectorOfReturn(String query,IEngine engine, Boolean raw){
		Vector<String> retString = new Vector<String>();
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(engine, query);
		//		wrap.execute();

		String[] names = wrap.getVariables();

		while (wrap.hasNext()) {
			ISelectStatement bs = wrap.next();
			Object value = null;
			if(raw){
				value = bs.getRawVar(names[0]);
			} else {
				value = bs.getVar(names[0]);
			}
			String val = null;
			if(value instanceof Binding){
				val = ((Value)((Binding) value).getValue()).stringValue();
			}
			else{
				val = value +"";
			}
			val = val.replace("\"", "");
			retString.addElement(val);
		}
		return retString;

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

	/**
	 * 
	 * @param engine
	 * @param nodesList
	 * @param getDisplayNames
	 * @return
	 */
	public static List getTransformedNodeNamesList(IEngine engine, List nodesList, boolean getDisplayNames){
		//array, list or vector support only

		for(int i = 0; i< nodesList.size(); i++){
			String currentUri =  nodesList.get(i).toString();
			String finalUri = Utility.getTransformedNodeName(engine, currentUri , getDisplayNames);
			if(!finalUri.equals(currentUri)){
				nodesList.set(i, finalUri);
			}
		}
		return nodesList;
	}

	/**
	 *  use for maps that map a string to a list of nodes...more commmonly used than I expected
	 * @param engine
	 * @param nodeMap
	 * @param getDisplayNames
	 * @return
	 */
	public static Map<String, List<Object>> getTransformedNodeNamesMap(IEngine engine, Map<String, List<Object>> nodeMap, boolean getDisplayNames){
		if(nodeMap!= null && nodeMap.size()>0){
			for(Map.Entry<String, List<Object>> eachMap: nodeMap.entrySet()){
				List<Object> binding = Utility.getTransformedNodeNamesList(engine, nodeMap.get(eachMap.getKey()),false);
			}
		}
		return nodeMap;
	}


	/**
	 * returns the physical or logical/display name
	 * loop through the first time using the qualified class name ie this assumes you got something like http://semoss.org/ontologies/Concept/Director/Clint_Eastwood
	 *     so getting the qualified name will give you http://semoss.org/ontologies/Concept/Director
	 * if the translated uri is the same (ie no translated name was found, so maybe you actually did come into this method with a nodeUri of http://semoss.org/ontologies/Concept/Director) 
	 *     on the second loop use that nodeUri directly and try to find a translated Uri
	 * 
	 * @param engine
	 * @param nodeUri
	 * @param getDisplayNames
	 * @return
	 */
	public static String getTransformedNodeName(IEngine engine, String nodeUri, boolean getDisplayNames){
		String finalUri = nodeUri;
		boolean getClassName = true;
		for(int j = 0; j < 2; j++){

			String fullUri = nodeUri;
			String uri = fullUri;

			if(getClassName){
				uri = Utility.getQualifiedClassName(uri);
				getClassName = false;
			}
			String physicalUri = engine.getTransformedNodeName(uri, getDisplayNames);
			if(!uri.equals(physicalUri)){
				finalUri = fullUri.replace(uri, physicalUri);
				break;
			}
		}
		return finalUri;
	}



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
			else if((retO = getDouble(input)) != null )
			{
				retObject = new Object[2];
				retObject[0] = "double";
				retObject[1] = retO;
			}
			else if((retO = getDate(input)) != null )// try dates ? - yummy !!
			{
				retObject = new Object[2];
				retObject[0] = "date";
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

	public static String getDate(String input)
	{
		String[] date_formats = {
				"yyyy-MM-dd",
				//"dd/MM/yyyy",
				"MM/dd/yyyy",
				//"dd-MM-yyyy",
				"yyyy/MM/dd", 
				"yyyy MMM dd",
				"yyyy dd MMM",
				"dd MMM yyyy",
				"dd MMM",
				"MMM dd",
				"dd MMM yyyy",
		"MMM yyyy"};

		String output_date = null;
		boolean itsDate = false;
		for (String formatString : date_formats)
		{
			try
			{    
				Date mydate = new SimpleDateFormat(formatString).parse(input);
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

	public static String getTimeStamp(String input)
	{
		String[] date_formats = {
				//"dd/MM/yyyy",
				"MM/dd/yyyy",
				//"dd-MM-yyyy",
				"yyyy-MM-dd",
				"yyyy/MM/dd", 
				"yyyy MMM dd",
				"yyyy dd MMM",
				"dd MMM yyyy",
				"dd MMM",
				"MMM dd",
				"dd MMM yyyy",
		"MMM yyyy"};

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
			e.printStackTrace();
		}

		return outDate;
	}


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
		try {
			Double num = Double.parseDouble(input);
			return num;
		} catch(NumberFormatException e) {
			return null;
		}
	}

	//this doesn't consider 1.2E8 etc.
	//    public static boolean isNumber(String input) {
	//    	//has digits, followed by optional period followed by digits
	//    	return input.matches("(\\d+)\\Q.\\E?(\\d+)?"); 
	//    }

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

		if(origDataType.equals("DOUBLE") || origDataType.equals("INT")) {
			return "DOUBLE";
		}

		if(origDataType.contains("DATE")) {
			return "DATE";
		}

		if(origDataType.contains("VARCHAR")) {
			return "STRING";
		}

		return "STRING";
	}

	public static String convertDataTypeToString(IMetaData.DATA_TYPES type) {
		if(type.equals(IMetaData.DATA_TYPES.NUMBER)) { 
			return "double";
		} else if(type.equals(IMetaData.DATA_TYPES.STRING)) {
			return "varchar(800)";
		} else {
			return "date";
		}
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

	public static IEngine loadWebEngine(String fileName, Properties prop) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		// load the engine
		IEngine engineToAdd = loadEngine(fileName, prop);
		// get the engine name
		String engineName = engineToAdd.getEngineName();

		// get the solr instance
		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
		// if the solr is active...
		if (solrE.serverActive()) {
			/*
			 * Here is the logic to determine if we need to load the engine into solr
			 * 
			 * 1) if the database is hidden -> do NOT add to solr
			 * Given that the database if not hidden, do the following
			 * 2) the user can define in the SMSS file a boolean value SOLR_RELOAD
			 * 		-> if the value is true, then we add the engine into solr
			 * 3) if a developer has hard coded to reload all solr values based on a hard coded boolean in abstract engine
			 * 		-> if the value is true, then we add the engine into solr
			 * 		-> note: this should be false whenever we make a build/deploy... 
			 * 					this is purely for ease in testing new development code

			 * 4) check to see if solr already contains the engine
			 * 		-> if the value is false, then we add the engine into solr
			 * 
			 * ****Decision Logic****
			 * Given that the engine is not hidden (i.e. 1 is false), then
			 * 		if a developer has hard coded to reload all solr values in abstract engine (2 is true) or 
			 * 		if the user has hard reloaded (2 is true) or 
			 * 		if solr doesn't contain the engine (3 is false), then
			 * 			add the engine into solr
			 */

			// 1) get the boolean is the database is hidden
			// 		default value is false if it is not found in the SMSS file
			String hiddenString = engineToAdd.getProperty(Constants.HIDDEN_DATABASE);
			boolean hidden = false;
			if (hiddenString != null) {
				hidden = Boolean.parseBoolean(hiddenString);
			}
			// 2) check if the user has set a hard reload to the SOLR_RELOAD boolean
			//		default value is false if it is not found in the SMSS file
			String smssPropString = engineToAdd.getProperty(Constants.SOLR_RELOAD);
			boolean smssProp = false;
			if (smssPropString != null) {
				smssProp = Boolean.parseBoolean(smssPropString);
			}
			// this if statement corresponds to the decision logic in comment block above
			// 3) and 4) are checked within the if statement
			if (!hidden && (AbstractEngine.RECREATE_SOLR || smssProp || !solrE.containsEngine(engineName))) {
				// alright, we are going to load the engines insights into solr
				LOGGER.info(engineToAdd.getEngineName() + " has solr force reload value of " + smssProp );
				LOGGER.info(engineToAdd.getEngineName() + " is reloading solr");
				try {
					// add the instances into solr
					addToSolrInstanceCore(engineToAdd);
					// add the insights into solr
					addToSolrInsightCore(engineToAdd);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			// if the engine is hidden, delete it from solr
			else if(hidden){
				Utility.deleteFromSolr(engineName);
			}
			// if the smss prop was set to true -> i.e. a hard solr reload for that specific engine
			// then we want to change the boolean to be false such that this is only a one time solr reload
			if(smssProp){
				LOGGER.info(engineToAdd.getEngineName() + " is changing solr boolean on smss");
				changeSMSSValue(fileName, Constants.SOLR_RELOAD, "false");
			}
		}

		// return the newly loaded engine
		return engineToAdd;
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
			//			boolean closeDB = false;
			String engineName = prop.getProperty(Constants.ENGINE);
			String engineClass = prop.getProperty(Constants.ENGINE_TYPE);

			if(engines.startsWith(engineName) || engines.contains(";"+engineName+";") || engines.endsWith(";"+engineName)) {
				LOGGER.debug("DB " + engineName + "<> is already loaded...");

				// engines are by default loaded so that we can keep track on the front end of engine/all call
				// so eventhough it is added here there is a good possibility it is not loaded so check to see this
				if(DIHelper.getInstance().getLocalProp(engineName) instanceof IEngine) 
					return (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			}

			// we need to store the smss location in DIHelper 
			DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.STORE, fileName);

			
			//TEMPORARY
			// TODO: remove this
			if(engineClass.equals("prerna.rdf.engine.impl.RDBMSNativeEngine")){
				engineClass = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
			}
			else if(engineClass.startsWith("prerna.rdf.engine.impl.")){
				engineClass = engineClass.replace("prerna.rdf.engine.impl.", "prerna.engine.impl.rdf.");
			}
			//			if(engineClass.contains("RDBMSNativeEngine")){
			//				closeDB = true; //close db
			//			}
			engine = (IEngine)Class.forName(engineClass).newInstance();
			engine.setEngineName(engineName);
			if(prop.getProperty("MAP") != null) {
				engine.addProperty("MAP", prop.getProperty("MAP"));
			}
			engine.openDB(fileName);
			//no point in doing this... it is set in the openDB call
			//			engine.setDreamer(prop.getProperty(Constants.DREAMER));
			//			engine.setOntology(prop.getProperty(Constants.ONTOLOGY));

			// set the core prop
			if(prop.containsKey(Constants.DREAMER))
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
			//			if(prop.containsKey(Constants.ONTOLOGY))
			//				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
			if(prop.containsKey(Constants.OWL)) {
				DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
				//engine.setOWL(prop.getProperty(Constants.OWL));
			}

			// set the engine finally
			engines = engines + ";" + engineName;
			DIHelper.getInstance().setLocalProperty(engineName, engine);
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);

			// now add or remove based on if it is hidden to local master
			boolean hidden = (prop.getProperty(Constants.HIDDEN_DATABASE) != null && Boolean.parseBoolean(prop.getProperty(Constants.HIDDEN_DATABASE)));
			boolean isLocal = engineName.equals(Constants.LOCAL_MASTER_DB_NAME);
			if(!hidden && !isLocal) {
				// I wonder if this should be done without loading the engine and all the paraphrenelia
				//addToLocalMaster(engine);
				synchronizeEngineMetadata(engineName);
			} else if(!isLocal){ // never add local master to itself...
				DeleteFromMasterDB deleter = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
				deleter.deleteEngine(engineName);
				Utility.deleteFromSolr(engineName);
			}
			// still need to find a way to move this forward.. 
			// but for now I am ignoring
			Utility.loadDataTypesIfNotPresent(engine, fileName);

			//			if(closeDB)
			//				engine.closeDB();
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
	 * Add data types into the OWL file for each concept and property for an engine 
	 * @param engine				The IEngine to add datatypes for
	 * @param fileName				The location of the SMSS file for that engine
	 */
	public static void loadDataTypesIfNotPresent(IEngine engine, String fileName) {
		/*
		 * Many use cases through the application requires that the data type be properly 
		 * defined within the OWL file for each data source
		 * 		-> when data type is not defined, most places in the code assumes values
		 * 			to be of type string/varchar
		 * 
		 * This routine is called at engine start-up to loop through every concept and
		 * property and add an appropriate data type triple
		 * 
		 * Note: this code first looks to see if a data type for that column exists within
		 * the owl, only if it not present will it try to determine the data type
		 * 
		 * Here is the logical flow
		 * 1) grab the boolean on the SMSS file to see if we need to go through this routine
		 * 2) if the boolean is true, continue to perform the following steps
		 * 3) grab all the concepts that exist in the database
		 * 4) get all the instances for the concept and determine the type IF a type is not present
		 * 		-> note: all concepts in an RDF database are automatically strings
		 * 			since they are stored as URIs
		 * 5) grab all the properties for the given concept
		 * 6) determine the type of the property IF a type is not already present
		 * 
		 * There is a very annoying caveat.. we have an annoying bifurcation based on the engine type
		 * If it is a RDBMS, getting the properties is pretty easy based on the way the IEngine is set up and how RDBMS queries work
		 * However, for RDF, getting the properties requires us to create a query and execute that query to get the list of values :/
		 */

		// 1) grab boolean value that was defined in the SMSS file to determine if we need to look at data types to add to owl
		boolean fillEmpty = true;
		String fillEmptyStr = engine.getProperty(Constants.FILL_EMPTY_DATATYPES);
		if(fillEmptyStr != null) {
			fillEmpty = Boolean.parseBoolean(fillEmptyStr);
		}

		// 2) if the boolean is true, proceed to perform logic, else, nothing to do
		if(fillEmpty) {
			LOGGER.info(engine.getEngineName() + " is reloading data types into owl file");
			// grab the engine type
			ENGINE_TYPE engineType = engine.getEngineType();
			// use the super handy owler object to actual add the triples 
			OWLER owler = new OWLER(engine, ((AbstractEngine) engine).getOWL());

			// 3) first grab all the concepts
			// see if concept has a data type, if not, determine the type and then add it
			Vector<String> concepts = engine.getConcepts2(false);;
			for(String concept : concepts) {
				// ignore stupid master concept
				if(concept.equals("http://semoss.org/ontologies/Concept")) {
					continue; 
				}
				String conceptType = engine.getDataTypes(concept);
				// 4) if the type of the concept isn't stored, need to add it
				if(conceptType == null) {
					// checking the type if rdf
					// if it is a RDF engine, all concepts are strings as its stored in a URI
					if(engineType != ENGINE_TYPE.RDBMS) {
						// all URIs are strings!!!!
						owler.addConcept(Utility.getInstanceName(concept), "STRING");
					} else {
						// grab all values
						Vector<Object> instances = engine.getEntityOfType(concept);
						if(instances != null && !instances.isEmpty()) {
							// determine the type via the first row
							String instanceObj = instances.get(0).toString().replace("\"", "");
							String type = Utility.findTypes(instanceObj)[0] + "";
							owler.addConcept(Utility.getInstanceName(concept), type);
						} else {
							// why is this a thing???
							LOGGER.error("no instances... not sure how i determine a type here...");
						}
					}
				}

				// 5) For the concept, get all the properties
				// see if property has a data type, if not, determine the type and then add it
				List<String> propNames = engine.getProperties4Concept2(concept, false);
				if(propNames != null && !propNames.isEmpty()) {
					// need a bifurcation in logic between rdbms and rdf
					// rdbms engine is smart enough to parse the table and column name from the uri in getEntityOfType call
					// however, rdf is dumb and requires a unique query to be created 
					// in order to get the values of a property for a specific concept
					for(String prop : propNames) {
						String propType = engine.getDataTypes(prop);

						// 6) If the prop type isn't sotred, need to add it
						if(propType == null) {
							if(engineType == ENGINE_TYPE.RDBMS) {
								// grab all values
								Vector<Object> properties = engine.getEntityOfType(prop);
								if(properties != null && !properties.isEmpty()) {
									// determine the type via the first row
									String property = properties.get(0).toString().replace("\"", "");
									String type = Utility.findTypes(property)[0] + "";
									owler.addProp(Utility.getInstanceName(concept), Utility.getInstanceName(prop), type);
								} else {
									// why is this a thing???
									LOGGER.error("no instances of property... not sure how i determine a type here...");
								}
							} else {
								// sadly, need to hand jam the appropriate query here
								String propQuery = "SELECT DISTINCT ?property WHERE { {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + concept + "> } { ?x <" + prop + "> ?property} }";
								ISelectWrapper propWrapper = WrapperManager.getInstance().getSWrapper(engine, propQuery);
								if(propWrapper.hasNext()) {
									ISelectStatement propSS = propWrapper.next();
									String property = propSS.getVar("property").toString().replace("\"", "");
									String type = Utility.findTypes(property)[0] + "";
									owler.addProp(Utility.getInstanceName(concept), Utility.getInstanceName(prop), type);
								}  else {
									// why is this a thing???
									LOGGER.error("no instances of property... not sure how i determine a type here...");
								}
							}
						}
					}
				}
			}

			// now write the owler with all these triples added
			// also need to reset the OWL within the engine to load in the triples
			try {
				owler.export();
				// setting the owl reloads the base engine to get the data types
				engine.setOWL(owler.getOwlPath());
			} catch (IOException e) {
				e.printStackTrace();
			}

			// update the smss file to contain the boolean as true to avoid this process on start up again
			LOGGER.info(engine.getEngineName() + " is changing boolean on smss for filling empty datatypes");
			changeSMSSValue(fileName, Constants.FILL_EMPTY_DATATYPES, "false");
		}
	}

	/**
	 * Adds the engine into the local master database
	 * @param engineToAdd				The engine to add into local master
	 */
	public static void addToLocalMaster(IEngine engineToAdd) {
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
		if(engineToAdd == null) {
			throw new NullPointerException("Engine passed in is null... no engine to load");
		}

		// generate the appropriate query to execute on the local master engine to get the time stamp
		String engineName = engineToAdd.getEngineName();
		String engineURL = "http://semoss.org/ontologies/Concept/Engine/" + Utility.cleanString(engineName, true);
		String localDbQuery = "SELECT DISTINCT ?TIMESTAMP WHERE {<" + engineURL + "> <" + BaseDatabaseCreator.TIME_KEY + "> ?TIMESTAMP}";

		// generate the query to execute on the engine's OWL to get the time stamp
		String engineQuery = "SELECT DISTINCT ?TIMESTAMP WHERE {<" + BaseDatabaseCreator.TIME_URL + "> <" + BaseDatabaseCreator.TIME_KEY + "> ?TIMESTAMP}";

		// 1) get the local master timestamp for engine
		String localDbTimeForEngine = null;
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(localMaster, localDbQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			localDbTimeForEngine = ss.getVar(names[0]) + "";
		}

		// 2) get the engine timestamp from OWL 
		String engineDbTime = null;
		ISelectWrapper wrapper2 = WrapperManager.getInstance().getSWrapper( ((AbstractEngine)engineToAdd).getBaseDataEngine(), engineQuery);
		String[] names2 = wrapper2.getVariables();
		while(wrapper2.hasNext()) {
			ISelectStatement ss = wrapper2.next();
			engineDbTime = ss.getVar(names2[0]) + "";
		}

		// if the engine OWL file doesn't have a time stamp, add one into it
		if(engineDbTime == null) {
			DateFormat dateFormat = BaseDatabaseCreator.getFormatter();
			Calendar cal = Calendar.getInstance();
			engineDbTime = dateFormat.format(cal.getTime());
			((AbstractEngine)engineToAdd).getBaseDataEngine().doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{BaseDatabaseCreator.TIME_URL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});
			((AbstractEngine)engineToAdd).getBaseDataEngine().commit();
			try {
				((AbstractEngine)engineToAdd).getBaseDataEngine().exportDB();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (RDFHandlerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 4) perform the necessary additions if the time stamps do not equal
		// this is broken out into 2 separate parts
		// 4.1) the local master doesn't have a time stamp which means the engine is not present
		//		-> i.e. we do not need to remove the engine and re-add it
		// 4.2) the time is present and we need to remove anything relating the engine that was in the engine and then re-add it

		if(localDbTimeForEngine == null) {
			// here we add the engine's time stamp into the local master
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			// logic to register the engine into the local master
			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal(engineToAdd);
			localMaster.commit();
		} else if(!localDbTimeForEngine.equals(engineDbTime)) {
			// remove the existing time stamp in the local master
			localMaster.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, localDbTimeForEngine, false});
			// add the time stamp to be equal to that which is stored in the engine's OWL
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			// if it has a time stamp, it means it was previously in local master
			// logic to delete an engine from the local master
			DeleteFromMasterDB remover = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			remover.deleteEngine(engineName);

			// logic to add the engine into the local master
			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal(engineToAdd);
			localMaster.commit();
		}
	}

	public static void synchronizeEngineMetadata(String engineName) {
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
		String engineFile = DIHelper.getInstance().getCoreProp().getProperty(engineName + "_" + Constants.STORE);

		// this has all the details
		// the engine file is primarily the SMSS that is going to be utilized for the purposes of retrieving all the data
		Properties prop = new Properties();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		FileInputStream fis = null;
		try {
			fis = new FileInputStream(engineFile);
			prop.load(fis);
//			prop.put("fis", fis);
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
		}
			}
		}

		String owlFileName = baseFolder + "/" + prop.getProperty(Constants.OWL);
		File owlFile = new File(owlFileName);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String engineDbTime = df.format(new Date(owlFile.lastModified()));

		String engineURL = "http://semoss.org/ontologies/meta/engine/" + Utility.cleanString(engineName, true);
		String localDbQuery = "SELECT DISTINCT ?TIMESTAMP WHERE {<" + engineURL + "> <http://semoss.org/ontologies/Relation/modified> ?TIMESTAMP}";

		// 1) get the local master timestamp for engine
		String localDbTimeForEngine = null;
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(localMaster, localDbQuery);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			localDbTimeForEngine = ss.getVar(names[0]) + "";
		}

		// if the engine OWL file doesn't have a time stamp, add one into it
		// not sure if I need this anymore
		/*
		if(engineDbTime == null) {
			DateFormat dateFormat = BaseDatabaseCreator.getFormatter();
			Calendar cal = Calendar.getInstance();
			engineDbTime = dateFormat.format(cal.getTime());
			((AbstractEngine)engineToAdd).getBaseDataEngine().doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{BaseDatabaseCreator.TIME_URL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});
			((AbstractEngine)engineToAdd).getBaseDataEngine().commit();
			try {
				((AbstractEngine)engineToAdd).getBaseDataEngine().exportDB();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (RDFHandlerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}*/

		// 4) perform the necessary additions if the time stamps do not equal
		// this is broken out into 2 separate parts
		// 4.1) the local master doesn't have a time stamp which means the engine is not present
		//		-> i.e. we do not need to remove the engine and re-add it
		// 4.2) the time is present and we need to remove anything relating the engine that was in the engine and then re-add it

		if(localDbTimeForEngine == null) {
			// here we add the engine's time stamp into the local master
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			// logic to register the engine into the local master
			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal2(prop);
			localMaster.commit(); 
		} else if(!localDbTimeForEngine.startsWith(engineDbTime)) { // BIGData has idiosyncracy where it keeps date in a weird format 2016-05-12T12:23:07.000Z instead of 2016-05-12T12:23:07
			// remove the existing time stamp in the local master
			localMaster.doAction(IEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, localDbTimeForEngine, false});
			// add the time stamp to be equal to that which is stored in the engine's OWL
			localMaster.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{engineURL, BaseDatabaseCreator.TIME_KEY, engineDbTime, false});

			// if it has a time stamp, it means it was previously in local master
			// logic to delete an engine from the local master
			DeleteFromMasterDB remover = new DeleteFromMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			remover.deleteEngine(engineName);

			// logic to add the engine into the local master
			AddToMasterDB adder = new AddToMasterDB(Constants.LOCAL_MASTER_DB_NAME);
			adder.registerEngineLocal2(prop);
			localMaster.commit();
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

		if(type == IEngine.ENGINE_TYPE.RDBMS)
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
	 * @param engineName - engine to get
	 * @return
	 * 
	 * Use this method to get the engine when the engine hasn't been loaded
	 */
	public static IEngine getEngine(String engineName) {
		IEngine engine = null;
		if(DIHelper.getInstance().getLocalProp(engineName) != null) {
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		} else {
			// start up the engine
			String smssFile = (String)DIHelper.getInstance().getCoreProp().getProperty(engineName + "_" + Constants.STORE);
			// start it up
			if(smssFile != null)
			{
				FileInputStream fis = null;
				try {
					Properties daProp = new Properties();
					fis = new FileInputStream(smssFile);
					daProp.load(fis);
					engine = Utility.loadWebEngine(smssFile, daProp);
					System.out.println("Loaded the engine.. !!!!! " + engineName);
				} catch (KeyManagementException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				} catch (KeyStoreException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if(fis != null) {
						try {
							fis.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			else
				System.out.println("There is no SMSS File for this engine.. ");
		}
		
		return engine;
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

}
