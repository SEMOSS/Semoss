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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.owasp.encoder.Encode;
import org.owasp.esapi.ESAPI;
import org.owasp.esapi.codecs.MySQLCodec;
import org.owasp.html.PolicyFactory;
import org.owasp.html.Sanitizers;
import org.quartz.CronExpression;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import com.google.common.base.Strings;
import com.google.common.net.InternetDomainName;
import com.google.gson.GsonBuilder;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.ZKClient;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IReactorEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.SmssUtilities;
import prerna.masterdatabase.AddToMasterDB;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.om.IStringExportProcessor;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.reactor.IReactor;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.tcp.PayloadStruct;
import prerna.tcp.SocketServerHandler;
import prerna.tcp.client.SocketClient;
import prerna.tcp.workers.EngineSocketWrapper;
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
public final class Utility {

	public static int id = 0;
	private static final Logger classLogger = LogManager.getLogger(Utility.class);
	private static final String SPECIFIED_PATTERN = "[@]{1}\\w+[-]*[\\w/.:]+[@]";
	
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
			classLogger.debug(data);
			// put something to strip the @
			paramHash.put(data, Constants.EMPTY);
		}

		return paramHash;
	}
	
	/**
	 * This is to remove scripts from being passed
	 * 
	 * @param stringToNormalize
	 * @return
	 */
	public static String inputSanitizer(String stringToNormalize) {
		if (stringToNormalize == null) {
			classLogger.debug("input to sanitzer is null, returning null");
			return stringToNormalize;
		}

		PolicyFactory policy = Sanitizers.FORMATTING.and(Sanitizers.LINKS).and(Sanitizers.BLOCKS).and(Sanitizers.STYLES)
				.and(Sanitizers.IMAGES).and(Sanitizers.TABLES);
		MySQLCodec mySQLCodec=new MySQLCodec(MySQLCodec.Mode.ANSI);
		return ESAPI.encoder().encodeForSQL(mySQLCodec, policy.sanitize(stringToNormalize));
	}


	/**
	 * This is to remove sql injection from strings
	 * 
	 * @param stringToNormalize
	 * @return
	 */
	public static String inputSQLSanitizer(String stringToNormalize) {
		if (stringToNormalize == null) {
			classLogger.debug("input to sanitzer is null, returning null");
			return stringToNormalize;
		}

		MySQLCodec mySQLCodec=new MySQLCodec(MySQLCodec.Mode.ANSI);
		return ESAPI.encoder().encodeForSQL(mySQLCodec, (stringToNormalize));
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

			classLogger.debug(data);
			// put something to strip the @
			paramHash.put(paramName, paramValue);
		}

		return paramHash;
	}
	
	/**
	 * Get the Base Folder
	 * @return
	 */
	public static String getBaseFolder() {
		return getDIHelperProperty(Constants.BASE_FOLDER);
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getInsightCacheDir() {
		return getDIHelperProperty(Constants.INSIGHT_CACHE_DIR);
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getCsvInsightCacheDir() {
		return getDIHelperProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
	}
	
	/**
	 * Get any property from DIHelper
	 * @param prop
	 * @return
	 */
	public static String getDIHelperProperty(String prop) {
		if(DIHelper.getInstance() == null) {
			return null;
		}
		return DIHelper.getInstance().getProperty(prop);
	}
	
	public static Object getDIHelperLocalProperty(String prop) {
		if(DIHelper.getInstance() == null) {
			return null;
		}
		return DIHelper.getInstance().getLocalProp(prop);
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

				classLogger.debug(data);
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
		classLogger.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key).get(0) + "";
			classLogger.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
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
		classLogger.debug("Param Hash is " + paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key);
//			classLogger.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
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

	public static String getFQNodeName(IDatabaseEngine engine, String URI) {
		if (engine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
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
		IDatabase engine = (IDatabase)DIHelper.getInstance().getLocalProp(repos[0]+"");
		// loads all of the labels
		// http://www.w3.org/2000/01/rdf-schema#label
		String labelQuery = "";
		
		//fill all uri for binding string
		StringBuffer bindingStr = new StringBuffer("");
		for (int i = 0; i<uri.size();i++)
		{
			if(engine.getEngineType() == IDatabase.DATABASE_TYPE.SESAME)
				bindingStr = bindingStr.append("(<").append(uri.get(i)).append(">)");
			else
				bindingStr = bindingStr.append("<").append(uri.get(i)).append(">");
		}
		Hashtable paramHash = new Hashtable();
		paramHash.put("FILTER_VALUES",  bindingStr.toString());
		if(engine.getEngineType() == IDatabase.DATABASE_TYPE.SESAME)
		{			
			labelQuery = "SELECT DISTINCT ?Entity ?Label WHERE " +
					"{{?Entity <http://www.w3.org/2000/01/rdf-schema#label> ?Label}" +
					"}" +"BINDINGS ?Entity {@FILTER_VALUES@}";
		}
		else if(engine.getEngineType() == IDatabase.DATABASE_TYPE.JENA)
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
	public static String getConceptType(IDatabaseEngine engine, String subjectURI) {
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
			IDatabaseEngine engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(s);
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

	public static boolean isFactorType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		if (dataType.startsWith("FACTOR") || dataType.startsWith("ORDER")) {
			return true;
		}

		return false;
	}

	/**
	 * Changes a value within the properties file for a given key
	 * 
	 * @param filePath
	 * @param keyToAlter
	 * @param valueToProvide
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, String keyToAlter, String valueToProvide) throws IOException {
		changePropertiesFileValue(filePath, keyToAlter, valueToProvide, false);
	}

	/**
	 * 
	 * @param filePath
	 * @param keyToAlter
	 * @param valueToProvide
	 * @param contains
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, String keyToAlter, String valueToProvide, boolean contains) throws IOException {
		Map<String, String> keyToNewValue = new HashMap<>();
		keyToNewValue.put(keyToAlter, valueToProvide);
		changePropertiesFileValue(filePath, keyToNewValue, contains);
	}
	
	/**
	 * 
	 * @param filePath
	 * @param keyToNewValue
	 * @param contains
	 * @throws IOException
	 */
	public static void changePropertiesFileValue(String filePath, Map<String, String> keyToNewValue, boolean contains) throws IOException {
		FileOutputStream fileOut = null;
		File file = new File(filePath);

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
				
				// separate out logic for contains vs exact match
				if (contains) {
					boolean updated = false;
					// 3) if this line starts with the key to alter
					FOUND_LOOP : for(String keyToAlter : keyToNewValue.keySet()) {
						if (content.get(i).contains(keyToAlter)) {
							// create new line to write using the key and the new value
							String newKeyValue = keyToAlter + "\t" + keyToNewValue.get(keyToAlter);
							fileOut.write(newKeyValue.getBytes());
							updated = true;
							break FOUND_LOOP;
						}
					}
					
					// 4) if it doesn't, just write the next line as is
					if(!updated) {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				} else {
					boolean updated = false;
					for(String keyToAlter : keyToNewValue.keySet()) {
						FOUND_LOOP : if (content.get(i).startsWith(keyToAlter + "\t") || content.get(i).startsWith(keyToAlter + " ")) {
							// create new line to write using the key and the new value
							String newKeyValue = keyToAlter + "\t" + keyToNewValue.get(keyToAlter);
							fileOut.write(newKeyValue.getBytes());
							updated = true;
							break FOUND_LOOP;
						}
					}

					// 4) if it doesn't, just write the next line as is
					if(!updated) {
						byte[] contentInBytes = content.get(i).getBytes();
						fileOut.write(contentInBytes);
					}
					// after each line, write a line break into the file
					fileOut.write(lineBreak);
				}
			}
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
			throw ioe;
		} finally {
			// close the readers
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}

			try {
				if (fileOut != null) {
					fileOut.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
		}
	}

	/**
	 * Adds a new key-value pair into a properties file
	 * @param propertiesFile
	 * @param locInFile
	 * @param mods
	 * @throws IOException
	 */
	public static void addKeysAtLocationIntoPropertiesFile(String propertiesFile, String locInFile, Map<String, String> mods) throws IOException {
		FileOutputStream fileOut = null;
		File file = new File(propertiesFile);

		/*
		 * 1) Loop through the properties file and add each line as a list of strings
		 * 2) iterate through the list of strings and write out each line
		 * 3) if the current line being printed starts with locInFile
		 * 		then the new key-value pair will be written right after it
		 * 4) if locInFile was never found, print at end of file
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

			boolean found = false;
			fileOut = new FileOutputStream(file);
			for (int i = 0; i < content.size(); i++) {
				// 2) write out each line into the file
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());

				// 3) if the last line printed matches that in locInFile, then write the new
				// key-value pair after
				if (content.get(i).startsWith(locInFile + "\t") || content.get(i).startsWith(locInFile + " ")) {
					found = true;
					for(String keyToAdd : mods.keySet()) {
						String newProp = keyToAdd + "\t" + mods.get(keyToAdd);
						fileOut.write(newProp.getBytes());
						fileOut.write("\n".getBytes());
					}
				}
			}
			
			if(!found) {
				fileOut.write("\n".getBytes());
				for(String keyToAdd : mods.keySet()) {
					String newProp = keyToAdd + "\t" + mods.get(keyToAdd);
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
			throw ioe;
		} finally {
			// close the readers
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}

			try {
				if (fileOut != null) {
					fileOut.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
		}
	}
	
	/**
	 * Makes sure that the file we are creating is in fact unique
	 * @param directory
	 * @param fileLocation
	 * @return
	 */
	public static String getUniqueFilePath(String directory, String fileLocation) {
		String fileName = Utility.normalizePath(FilenameUtils.getBaseName(fileLocation).trim());
		String fileExtension = FilenameUtils.getExtension(fileLocation).trim();
		
		// h2 is weird and will not work if it doesn't end in .mv.db
		boolean isH2 = fileLocation.endsWith(".mv.db");
		File f = new File(directory + "/" + fileName + "." + fileExtension);
		int counter = 2;
		while(f.exists()) {
			if(isH2) {
				f = new File(directory + "/" + fileName.replace(".mv", "") + " (" + counter + ")" + ".mv.db");
			} else {
				f = new File(directory + "/" + fileName + " (" + counter + ")" + "." + fileExtension);
			}
			counter++;
		}
		
		return f.getAbsolutePath();
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
			classLogger.error(Constants.STACKTRACE, ioe);
		} finally {
			try {
				if (newExcelFile != null) {
					newExcelFile.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
		}
	}

	public static String retrieveResult(String api, Hashtable<String, String> params) {
		String output = "";
		BufferedReader stream = null;
		InputStreamReader inputStream = null;
		CloseableHttpClient httpclient = null;
		try {
			URIBuilder uri = new URIBuilder(api);

			classLogger.debug("Getting data from the API...  " + api);
			classLogger.debug("Params is " + params);

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
			classLogger.error(Constants.STACKTRACE, ex);
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		} catch (NoSuchAlgorithmException nsae) {
			classLogger.error(Constants.STACKTRACE, nsae);
		} catch (KeyStoreException kse) {
			classLogger.error(Constants.STACKTRACE, kse);
		} catch (URISyntaxException use) {
			classLogger.error(Constants.STACKTRACE, use);
		} catch (KeyManagementException kme) {
			classLogger.error(Constants.STACKTRACE, kme);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
				if (stream != null)
					stream.close();
			} catch (IOException e) {
				classLogger.error("Error closing input stream for image");
			}
			try {
				if (httpclient != null)
					httpclient.close();
				if (stream != null)
					stream.close();
			} catch (IOException e) {
				classLogger.error("Error closing socket for httpclient");
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

			classLogger.info("Getting data from the API...  " + Utility.cleanLogString(api));
			classLogger.info("Params are " + Utility.cleanLogMap(params, "HASHTABLE"));

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
			classLogger.error(Constants.STACKTRACE, ex);
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		} catch (NoSuchAlgorithmException nsae) {
			classLogger.error(Constants.STACKTRACE, nsae);
		} catch (KeyStoreException kse) {
			classLogger.error(Constants.STACKTRACE, kse);
		} catch (URISyntaxException use) {
			classLogger.error(Constants.STACKTRACE, use);
		} catch (KeyManagementException kme) {
			classLogger.error(Constants.STACKTRACE, kme);
		}
		return null;
	}

	public static ISelectWrapper processQuery(IDatabaseEngine engine, String query) {
		classLogger.debug("PROCESSING QUERY: " + query);

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

	/**
	 * Gets the vector of uris from first variable returned from the query
	 * 
	 * @param raw    TODO
	 * @param sparql
	 * @param eng
	 * @return Vector of uris associated with first variale returned from the query
	 */
	public static Vector<String> getVectorOfReturn(String query, IDatabaseEngine engine, Boolean raw) {
		Vector<String> retArray = new Vector<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			while (wrapper.hasNext()) {
				Object[] values = null;
				if (raw) {
					values = wrapper.next().getRawValues();
				} else {
					values = wrapper.next().getValues();
				}

				if (values[0] != null) {
					retArray.add(values[0].toString());
				} else {
					retArray.add(null);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
	public static Vector<String[]> getVectorArrayOfReturn(String query, IDatabaseEngine engine, Boolean raw) {
		Vector<String[]> retArray = new Vector<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			while (wrapper.hasNext()) {
				Object[] values = null;
				if (raw) {
					values = wrapper.next().getRawValues();
				} else {
					values = wrapper.next().getValues();
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return retArray;
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

	public static IPlaySheet getPlaySheet(IDatabaseEngine engine, String psName) {
		classLogger.info("Trying to get playsheet for " + Utility.cleanLogString(psName));
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

	public static IDataMaker getDataMaker(IDatabaseEngine engine, String dataMakerName) {
		classLogger.info("Trying to get data maker for " + Utility.cleanLogString(dataMakerName));
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

	public static ISEMOSSTransformation getTransformation(IDatabaseEngine engine, String transName) {
		classLogger.info("Trying to get transformation for " + Utility.cleanLogString(transName));
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

	public static ISEMOSSAction getAction(IDatabaseEngine engine, String actionName) {
		classLogger.info("Trying to get action for " + Utility.cleanLogString(actionName));
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
			classLogger.debug("Dataframe name is " + Utility.cleanLogString(className));
			obj = Class.forName(className).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException cnfe) {
			classLogger.error(Constants.STACKTRACE, cnfe);
			classLogger.fatal("No such class: " + Utility.cleanLogString(className));
		} catch (InstantiationException ie) {
			classLogger.error(Constants.STACKTRACE, ie);
			classLogger.fatal("Failed instantiation: " + Utility.cleanLogString(className));
		} catch (IllegalAccessException iae) {
			classLogger.error(Constants.STACKTRACE, iae);
			classLogger.fatal("Illegal Access: " + Utility.cleanLogString(className));
		} catch (IllegalArgumentException iare) {
			classLogger.error(Constants.STACKTRACE, iare);
			classLogger.fatal("Illegal argument: " + Utility.cleanLogString(className));
		} catch (InvocationTargetException ite) {
			classLogger.error(Constants.STACKTRACE, ite);
			classLogger.fatal("Invocation exception: " + Utility.cleanLogString(className));
		} catch (NoSuchMethodException nsme) {
			classLogger.error(Constants.STACKTRACE, nsme);
			classLogger.fatal("No constructor: " + Utility.cleanLogString(className));
		} catch (SecurityException se) {
			classLogger.error(Constants.STACKTRACE, se);
			classLogger.fatal("Security exception: " + Utility.cleanLogString(className));
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

	public static Map<Integer, Set<Integer>> getCardinalityOfValues(String[] newHeaders, Map<String, Set<String>> edgeHash) {
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
				|| dataType.startsWith("SIGNED") || dataType.startsWith("SERIAL")

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
		if (dataType.startsWith("NUMBER") 
				|| dataType.startsWith("MONEY") 
				|| dataType.startsWith("SMALLMONEY")
				|| dataType.startsWith("DECIMAL") 
				|| dataType.startsWith("DEC")
				|| dataType.startsWith("NUMERIC")
				|| dataType.startsWith("DOUBLE") 
				|| dataType.startsWith("PRECISION") 
				|| dataType.startsWith("FLOAT")
				|| dataType.startsWith("FLOAT8")
				// REAL TYPE
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
	 * 
	 * @param prop
	 * @param filePath
	 * @param engineId
	 */
	public static void catalogEngineByType(String smssFilePath, Properties smssProp, String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";

		boolean syncToLocalMaster = false;

		DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath);
		IEngine.CATALOG_TYPE engineType = null;
		String rawType = smssProp.get(Constants.ENGINE_TYPE).toString();
		try {
			IEngine emptyClass = (IEngine) Class.forName(rawType).newInstance();
			engineType = emptyClass.getCatalogType();
			// FOR NOW
			// PGVECTOR IS A DATABASE ENGINE
			// BUT NO OWL
			if(emptyClass instanceof IDatabaseEngine && !(emptyClass instanceof IVectorDatabaseEngine)) {
				syncToLocalMaster = true;
			}
		} catch(Exception e) {
			classLogger.warn("Unknown class name = " + rawType + " in smss file " + smssFilePath);
		}
		
		DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.TYPE, engineType);
		String engineNames = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		if(!(engines.startsWith(engineId) || engines.contains(";"+engineId+";") || engines.endsWith(";"+engineId))) {
			engineNames = engineNames + ";" + engineId;
			DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineNames);
		}

		classLogger.info("Loading engine " + engineId + " of type = " + engineType);
		if(syncToLocalMaster) {
			// sync up the engine metadata now
			Utility.synchronizeEngineMetadata(engineId);
		}
		SecurityEngineUtils.addEngine(engineId, null);
	}
	
	/**
	 * Loads an engine and opens it
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	private static IEngine loadEngine(String smssFilePath, Properties smssProp) {
		IEngine engine = null;
		try {
			String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
			String engineId = smssProp.getProperty(Constants.ENGINE);
			String engineClass = smssProp.getProperty(Constants.ENGINE_TYPE);

			if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";") || engines.endsWith(";" + engineId)) {
				classLogger.debug("Engine " + engineId + " is already loaded...");
				// engines are by default loaded so that we can keep track on the front end of
				// engine/all call
				// so even though it is added here there is a good possibility it is not loaded
				// so check to see this
				if (DIHelper.getInstance().getEngineProperty(engineId) instanceof IDatabaseEngine) {
					return (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
				}
			}

			// we store the smss location in DIHelper
			if(smssFilePath != null) {
				DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFilePath);
			}
			// we also store the OWL location
			if (smssProp.containsKey(Constants.OWL)) {
				DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.OWL, smssProp.getProperty(Constants.OWL));
			}

			// create and open the class
			engine = (IEngine) Class.forName(engineClass).newInstance();
			engine.setEngineId(engineId);
			if(smssFilePath == null) {
				engine.open(smssProp);
			} else {
				engine.open(smssFilePath);
			}
			// set the engine in DIHelper
			DIHelper.getInstance().setEngineProperty(engineId, engine);

			// Append the engine name to engines if not already present
			if (!(engines.startsWith(engineId) 
					|| engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId))
					) {
				engines = engines + ";" + engineId;
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines);
			}
			
			if(DIHelper.getInstance().getLocalProp("core") == null 
					|| Boolean.parseBoolean(DIHelper.getInstance().getLocalProp("core")+"")) {
				// for database, load into local master as well
				if(engine.getCatalogType() == IEngine.CATALOG_TYPE.DATABASE) {
					boolean isLocal = engineId.equals(Constants.LOCAL_MASTER_DB);
					boolean isSecurity = engineId.equals(Constants.SECURITY_DB);
					boolean isScheduler = engineId.equals(Constants.SCHEDULER_DB);
					boolean isThemes = engineId.equals(Constants.THEMING_DB);
					boolean isUserTracking = engineId.equals(Constants.USER_TRACKING_DB);

					if (!isLocal && !isSecurity && !isScheduler && !isThemes && !isUserTracking) {
						synchronizeEngineMetadata(engineId);
					}
				}
				
				// always load into security
				SecurityEngineUtils.addEngine(engineId, null);
			}
		} catch (Exception e) {
			// null out the engine
			engine = null;
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}
	
	/**
	 * Loads a database - synchronizes to local master and security
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IDatabaseEngine loadDatabase(String smssFilePath, Properties smssProp) {
		IDatabaseEngine engine = null;
		try {
			engine = (IDatabaseEngine) loadEngine(smssFilePath, smssProp);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}
	
	/**
	 * Loads a storage - synchronizes to security
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IStorageEngine loadStorage(String smssFilePath, Properties smssProp) {
		IStorageEngine engine = null;
		try {
			engine = (IStorageEngine) loadEngine(smssFilePath, smssProp);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}

	/**
	 * Loads a model - synchronizes to security
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IModelEngine loadModel(String smssFilePath, Properties smssProp) {
		IModelEngine engine = null;
		try {
			engine = (IModelEngine) loadEngine(smssFilePath, smssProp);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}
	
	/**
	 * Loads a vector db - synchronizes to security
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IVectorDatabaseEngine loadVectorDatabase(String smssFilePath, Properties smssProp) {
		IVectorDatabaseEngine engine = null;
		try {
			engine = (IVectorDatabaseEngine) loadEngine(smssFilePath, smssProp);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}
	
	/**
	 * Loads a service engine - synchronizes to security
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IFunctionEngine loadServiceEngine(String smssFilePath, Properties smssProp) {
		IFunctionEngine engine = null;
		try {
			engine = (IFunctionEngine) loadEngine(smssFilePath, smssProp);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return engine;
	}
	
	/**
	 * 
	 * @param smssFilePath
	 * @param smssProp
	 * @return
	 */
	public static IProject loadProject(String smssFilePath, Properties smssProp) {
		IProject project = null;
		try {
			String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";
			String projectId = smssProp.getProperty(Constants.PROJECT);
			String projectClass = smssProp.getProperty(Constants.PROJECT_TYPE);

			if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";") || projects.endsWith(";" + projectId)) {
				classLogger.debug("Project " + projectId + " is already loaded...");
				// engines are by default loaded so that we can keep track on the front end of
				// engine/all call
				// so even though it is added here there is a good possibility it is not loaded
				// so check to see this
				if (DIHelper.getInstance().getProjectProperty(projectId) instanceof IProject) {
					return (IProject) DIHelper.getInstance().getProjectProperty(projectId);
				}
			}

			// clean up the SMSS files
			{
				try {
					Properties props = Utility.loadProperties(smssFilePath);
					boolean isAsset = Boolean.parseBoolean(props.getProperty(Constants.IS_ASSET_APP)+"");
					if(!isAsset && props.get(Settings.PUBLIC_HOME_ENABLE) == null) {
						classLogger.info("Updating project smss to include public home property");
						Map<String, String> mods = new HashMap<>();
						mods.put(Settings.PUBLIC_HOME_ENABLE, "false");
						Utility.addKeysAtLocationIntoPropertiesFile(smssFilePath, Constants.CONNECTION_URL, mods);
						// push to cloud
						ClusterUtil.pushProjectSmss(projectId);
					}
				} catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					//ignore
				}
			}
			
			// we store the smss location in DIHelper
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFilePath);

			// create and open the class
			project = (IProject) Class.forName(projectClass).newInstance();
			project.setProjectId(projectId);
			project.open(smssFilePath);

			// set the engine in DIHelper
			DIHelper.getInstance().setProjectProperty(projectId, project);

			// Append the engine name to engines if not already present
			if (!(projects.startsWith(projectId) || projects.contains(";" + projectId + ";")
					|| projects.endsWith(";" + projectId))) {
				projects = projects + ";" + projectId;
				DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
			}
			
			// add the project if not an asset
			if(!project.isAsset()) {
				SecurityProjectUtils.addProject(projectId, null);
			}
		} catch (InstantiationException ie) {
			classLogger.error(Constants.STACKTRACE, ie);
		} catch (IllegalAccessException iae) {
			classLogger.error(Constants.STACKTRACE, iae);
		} catch (ClassNotFoundException cnfe) {
			classLogger.error(Constants.STACKTRACE, cnfe);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return project;
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
		IDatabaseEngine localMaster = (IDatabaseEngine) DIHelper.getInstance().getEngineProperty(Constants.LOCAL_MASTER_DB);
		if (localMaster == null) {
			classLogger.info(">>>>>>>> Unable to find local master database in DIHelper.");
			return;
		}

		// generate the appropriate query to execute on the local master engine to get
		// the time stamp
		String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";

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

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Date rdbmsDate = MasterDatabaseUtility.getEngineDate(engineId);
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
			AddToMasterDB adder = new AddToMasterDB();
			// logic to register the engine into the local master
			adder.registerEngineLocal(prop);
		} else if (!engineRdbmsDbTime.equalsIgnoreCase(engineDbTime)) {
			// if it has a time stamp, it means it was previously in local master
			// logic to delete an engine from the local master
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
			// logic to add the engine into the local master
			AddToMasterDB adder = new AddToMasterDB();
			adder.registerEngineLocal(prop);
		}
		
		// clear the caching of engine metadata
		EngineSyncUtility.clearEngineCache(engineId);
	}

	public static String getRandomString(int len) {
		String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";

		String retString = "a";
		SecureRandom secureRandom = new SecureRandom();
		for (int i = 0; i < len; i++) {
			double num = secureRandom.nextInt(alpha.length());
			retString = retString + alpha.charAt(new Double(num).intValue());
		}

		return retString;
	}

	public static boolean engineLoaded(String engineId) {
		return DIHelper.getInstance().getEngineProperty(engineId) != null;
	}
	
	public static boolean projectLoaded(String projectId) {
		return DIHelper.getInstance().getProjectProperty(projectId) != null;
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public static IProject getProject(String projectId) {
		return getProject(projectId, true);
	}
	
	/**
	 * 
	 * @param projectId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IProject getProject(String projectId, boolean pullIfNeeded) {
		IProject project = null;
		
		if((DIHelper.getInstance().getLocalProp("core") == null || DIHelper.getInstance().getLocalProp("core").toString().equalsIgnoreCase("true")))
		{
			if(DIHelper.getInstance().getProjectProperty(projectId) != null) {
				project = (IProject) DIHelper.getInstance().getProjectProperty(projectId);
			} else {
				// Acquire the lock on the engine,
				// don't want several calls to try and load the engine at the same
				// time
				classLogger.info("Applying lock for project " + Utility.cleanLogString(projectId));
				ReentrantLock lock = null;
				try {
					lock = ProjectSyncUtility.getProjectLock(projectId);
					lock.lock();
					classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is locked");
					
					// Need to do a double check here,
					// so if a different thread was waiting for the engine to load,
					// it doesn't go through this process again
					if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
						return (IProject) DIHelper.getInstance().getProjectProperty(projectId);
					}
					
					// If in a clustered environment, then pull the app first
					// TODO >>>timb: need to pull sec and lmd each time. They also need
					// correct jdbcs...
					if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
						ClusterUtil.pullProject(projectId);
					}
	
					// Now that the app has been pulled, grab the smss file
					String smssFile = (String) DIHelper.getInstance().getProjectProperty(projectId + "_" + Constants.STORE);
	
					// Start up the engine using the details in the smss
					if (smssFile != null) {
						// actual load engine process
						project = Utility.loadProject(smssFile, Utility.loadProperties(smssFile));
					} else {
						classLogger.debug("There is no SMSS File for the project " + projectId + "...");
					}
				} finally {
					if(lock != null) {
						// Make sure to unlock now
						lock.unlock();
						classLogger.info("Project "+ Utility.cleanLogString(projectId) + " is unlocked");
					}
				}
			}
		}
		else
		{
			String projectSock = projectId + "__SOCKET";
			
			if(DIHelper.getInstance().getProjectProperty(projectSock) != null) {
				project = (IProject) DIHelper.getInstance().getProjectProperty(projectSock);
			}
		}
		
		return project;
	}
	
	public static IProject getUserAssetWorkspaceProject(String projectId, boolean isAsset) {
		IProject project = null;
		
		if(DIHelper.getInstance().getProjectProperty(projectId) != null) {
			project = (IProject) DIHelper.getInstance().getProjectProperty(projectId);
		} else {
			// Acquire the lock on the engine,
			// don't want several calls to try and load the engine at the same
			// time
			classLogger.info("Applying lock for user asset/workspace " + projectId);
			ReentrantLock lock = ProjectSyncUtility.getProjectLock(projectId);
			lock.lock();
			classLogger.info("User asset/workspace "+ projectId + " is locked");

			try {
				// Need to do a double check here,
				// so if a different thread was waiting for the engine to load,
				// it doesn't go through this process again
				if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
					return (IProject) DIHelper.getInstance().getProjectProperty(projectId);
				}
				
				// If in a clustered environment, then pull the project first
				// TODO >>>timb: need to pull sec and lmd each time. They also need
				// correct jdbcs...
				if (ClusterUtil.IS_CLUSTER) {
					ClusterUtil.pullUserWorkspace(projectId, isAsset, false);
				}

				// Now that the app has been pulled, grab the smss file
				String folderName = null;
				if(isAsset) {
					folderName = "Asset";
				} else {
					folderName = "Workplace";
				}
				String smssFile = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
						+ "/" + Constants.USER_FOLDER + "/" + SmssUtilities.getUniqueName(folderName, projectId) + ".smss";
				// Start up the engine using the details in the smss
				if (smssFile != null && new File(Utility.normalizePath(smssFile)).exists()) {
					// actual load engine process
					project = Utility.loadProject(smssFile, Utility.loadProperties(Utility.normalizePath(smssFile)));
				} else {
					classLogger.debug("There is no SMSS File for the user asset/workspace " + projectId + "...");
				}
			} finally {
				// Make sure to unlock now
				lock.unlock();
				classLogger.info("User asset/workspace "+ projectId + " is unlocked");
			}
		}
		
		return project;
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IEngine getEngine(String engineId) {
		return getEngine(engineId, true);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IEngine getEngine(String engineId, boolean pullIfNeeded) {
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		IEngine.CATALOG_TYPE type = (IEngine.CATALOG_TYPE) typeAndSubtype[0];
		return getEngine(engineId, type, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IEngine getEngine(String engineId, IEngine.CATALOG_TYPE type, boolean pullIfNeeded) {
		if(IEngine.CATALOG_TYPE.DATABASE == type) {
			return getDatabase(engineId, pullIfNeeded);
		} else if(IEngine.CATALOG_TYPE.STORAGE == type) {
			return getStorage(engineId, pullIfNeeded);
		} else if(IEngine.CATALOG_TYPE.MODEL == type) {
			return getModel(engineId, pullIfNeeded);
		} else if(IEngine.CATALOG_TYPE.VECTOR == type) {
			return getVectorDatabase(engineId, pullIfNeeded);
		} else if(IEngine.CATALOG_TYPE.FUNCTION == type) {
			return getFunctionEngine(engineId, pullIfNeeded);
		} else if(IEngine.CATALOG_TYPE.VENV == type) {
			return getVenvEngine(engineId, pullIfNeeded);
		}
		
		throw new IllegalArgumentException("Unknown engine type with value " + type);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	private static IEngine baseGetEngine(String engineId, boolean pullIfNeeded) {
		IEngine engine = null;
		
		// Now that the database has been pulled, grab the smss file
		String smssFile = null;
		boolean reloadDB = false;
		Properties prop = null;
		
		boolean runningInCore = DIHelper.getInstance().getLocalProp("core") == null 
				|| Boolean.parseBoolean(DIHelper.getInstance().getLocalProp("core")+"");
		
		if(runningInCore) {
			// not sure why we need this after the first time but hey
			smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		}
		else // this is happening on the socket side
		{
			// on the socket side
			// it will pull the smss
			// and reload the engine
			// once reloaded it will be present in the DI Helper
			// check DI Helper to see if this is needed
			// if not try to figure if reload is required
			if(DIHelper.getInstance().getEngineProperty(engineId) == null) // if already loaded.. no need to load again
			{
				prop = getEngineDetails(engineId);
				if(prop != null)
				{
					reloadDB = true; 
				}
			}
			else
			{
				// this is already a loaded engine
				// engine socket wrapper is not persisted in the cache so we are all set here
				reloadDB = true;
			}
		}
		
		//logger.info("Reload DB is set to " + reloadDB);

		if( runningInCore || reloadDB ) {
			// If the engine has already been loaded, then return it
			// Don't acquire the lock here, because that would slow things down
			if (DIHelper.getInstance().getEngineProperty(engineId) != null) {
				engine = (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
			} else {
				// Acquire the lock on the engine,
				// don't want several calls to try and load the engine at the same
				// time
				classLogger.info("Applying lock for engine " + Utility.cleanLogString(engineId) + " to pull");
				ReentrantLock lock = null;
				try {
					lock = EngineSyncUtility.getEngineLock(engineId);
					lock.lock();
					classLogger.info("Engine "+ Utility.cleanLogString(engineId) + " is locked");
		
					// Need to do a double check here,
					// so if a different thread was waiting for the engine to load,
					// it doesn't go through this process again
					if (DIHelper.getInstance().getEngineProperty(engineId) != null) {
						return (IEngine) DIHelper.getInstance().getEngineProperty(engineId);
					}
					
					if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
						ClusterUtil.pullEngine(engineId);
					}
					
					// Now that the database has been pulled, grab the smss file
					smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
					
					// Start up the engine using the details in the smss
					if (smssFile != null) {
						// actual load engine process
						engine = Utility.loadEngine(smssFile, Utility.loadProperties(smssFile));
					}
					else if(prop != null)
					{
						engine = Utility.loadEngine(null, prop);	
					} else {
						classLogger.info("There is no SMSS File for the engine " + Utility.cleanLogString(engineId) + "...");
						classLogger.info("There is no SMSS File for the engine " + Utility.cleanLogString(engineId) + "...");
						classLogger.info("There is no SMSS File for the engine " + Utility.cleanLogString(engineId) + "...");
						classLogger.info("There is no SMSS File for the engine " + Utility.cleanLogString(engineId) + "...");
					}
	
					// TODO >>>timb: Centralize this ZK env check stuff and use is cluster variable
					// TODO >>>timb: remove node exists error or catch it
					// TODO >>>cluster: tag
					// Start with because the insights RDBMS has the id security_InsightsRDBMS
					if (!(engineId.startsWith("security") || engineId.startsWith("LocalMasterDatabase")
							|| engineId.startsWith("form_builder_engine") || engineId.startsWith("themes") || engineId.startsWith("scheduler") 
							|| engineId.startsWith("UserTrackingDatabase") )) {
						Map<String, String> envMap = System.getenv();
						if (envMap.containsKey(ZKClient.ZK_SERVER)
								|| envMap.containsKey(ZKClient.ZK_SERVER.toUpperCase())) {
							if (ClusterUtil.LOAD_ENGINES_LOCALLY) {
	
								// Only publish if actually loading on this box
								// TODO >>>timb: this logic only works insofar as we are assuming a user-based
								// docker layer in addition to the app containers
								String host = "unknown";
	
								if (envMap.containsKey(ZKClient.HOST)) {
									host = envMap.get(ZKClient.HOST);
								}
								
								if (envMap.containsKey(ZKClient.HOST.toUpperCase())) {
									host = envMap.get(ZKClient.HOST.toUpperCase());
								}
								
								// we are in business
								ZKClient client = ZKClient.getInstance();
								client.publishDB(engineId + "@" + host);
							}
						}
					}
				} finally {
					// Make sure to unlock now
					if(lock != null) {
						lock.unlock();
						classLogger.info("Engine "+ Utility.cleanLogString(engineId) + " is unlocked");
					}
				}
			}
			// send the information of engine to the smssfile to the socket
		}
		else // this is happening on the socket side
		{
			engine = new EngineSocketWrapper(engineId, (SocketServerHandler)DIHelper.getInstance().getLocalProp("SSH"));
		}
		return engine;
	}
	
	/**
	 * 
	 * @param engineId - engine to get
	 * @return
	 *         Use this method to get the engine when the engine hasn't been loaded
	 */
	public static IDatabaseEngine getDatabase(String engineId) {
		return getDatabase(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IDatabaseEngine getDatabase(String engineId, boolean pullIfNeeded) {
		return (IDatabaseEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IStorageEngine getStorage(String engineId) {
		return getStorage(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IStorageEngine getStorage(String engineId, boolean pullIfNeeded) {
		return (IStorageEngine) baseGetEngine(engineId, pullIfNeeded);
	}

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IModelEngine getModel(String engineId) {
		return getModel(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IModelEngine getModel(String engineId, boolean pullIfNeeded) {
		return (IModelEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IVectorDatabaseEngine getVectorDatabase(String engineId) {
		return getVectorDatabase(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IVectorDatabaseEngine getVectorDatabase(String engineId, boolean pullIfNeeded) {
		return (IVectorDatabaseEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IReactorEngine getReactorEngine(String engineId) {
		return getReactorEngine(engineId, true);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IReactorEngine getReactorEngine(String engineId, boolean pullIfNeeded) {
		return (IReactorEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IFunctionEngine getFunctionEngine(String engineId) {
		return getFunctionEngine(engineId, true);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IFunctionEngine getFunctionEngine(String engineId, boolean pullIfNeeded) {
		return (IFunctionEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IVenvEngine getVenvEngine(String engineId) {
		return getVenvEngine(engineId, true);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IVenvEngine getVenvEngine(String engineId, boolean pullIfNeeded) {
		return (IVenvEngine) baseGetEngine(engineId, pullIfNeeded);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static boolean isEngineLoaded(String engineId) {
		return DIHelper.getInstance().getEngineProperty(engineId) != null;
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static Properties getEngineDetails(String engineId)
	{
		// get the engine properties file
		// get the engine owl file
		// set the owl file location and start up
		
		CaseInsensitiveProperties  prop = null;
		try
		{
			SocketServerHandler ssh = (SocketServerHandler)DIHelper.getInstance().getLocalProp("SSH");
			PayloadStruct ps = new PayloadStruct();
			ps.epoc = "t1";
			ps.operation = ps.operation.ENGINE;
			ps.objId = engineId;
			ps.methodName = "getOrigProp";
			ps.longRunning = true;
			ps.response = false;
			ps.payloadClasses = new Class[] {};
			
			PayloadStruct response = ssh.writeResponse(ps);
			prop =  (CaseInsensitiveProperties)response.payload[0];
			
			// get the owl file and replace into properties
			ps = new PayloadStruct();
			ps.epoc = "t2";
			ps.operation = ps.operation.ENGINE;
			ps.objId = engineId;
			ps.methodName = "getOwl";
			ps.longRunning = true;
			ps.response = false;
			ps.payloadClasses = new Class[] {};
			
			response = ssh.writeResponse(ps);
			String owl = response.payload[0] + "";
			
			// find if the properties has a reload db on it
			boolean reload = false;
			if(prop.containsKey(Settings.LOAD_DB_ON_SOCKET)) {
				reload = prop.getProperty(Settings.LOAD_DB_ON_SOCKET).equalsIgnoreCase("True");
			}
			if(reload) {
				// write the owl file
				// replace it in the properties
				// write the properties file or not
				// return the properties
				String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
				File engineDir = new File(Utility.normalizeParam(baseFolder) + File.separator + "engines" + File.separator + engineId);
				if(!engineDir.exists()) {
					engineDir.mkdirs();
				}
				
				File owlFile = new File(engineDir.getAbsolutePath() + File.separator + engineId + ".owl");
				// engine owlFileName
				FileUtils.writeStringToFile(owlFile, owl);
				prop.replace(Constants.OWL, owlFile.getAbsolutePath());
								
				// give the properties back
				// write the properties file as well
				//File propFile = new File(engineDir.getAbsolutePath() + File.separator + engineId + ".smss");
				// write the propfile also into it
				//prop.put("PROP_FILE_LOCATION", propFile.getAbsolutePath());
				//FileWriter fw = new FileWriter(propFile);
//				prop.list(new PrintWriter(fw));
//				fw.flush();
//				fw.close();
			}
		} catch(Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}
		
		return prop;
	}

	/**
	 * PLEASE USE PortAllocator.getInstance().getNextAvailablePort()
	 */
//	public static String findOpenPort() {
//		classLogger.info("Finding an open port.. ");
//		boolean found = false;
//
//		int lowPort = 5355;
//		int highPort = lowPort + 10_000;
//
//		if (DIHelper.getInstance().getProperty("LOW_PORT") != null) {
//			try {lowPort = Integer.parseInt(DIHelper.getInstance().getProperty("LOW_PORT")); } catch (Exception ignore) {};
//		}
//		
//		if (DIHelper.getInstance().getProperty("HIGH_PORT") != null) {
//			try {highPort = Integer.parseInt(DIHelper.getInstance().getProperty("HIGH_PORT")); } catch (Exception ignore) {};
//		}
//		
//		for (; !found && lowPort < highPort; lowPort++) {
//			classLogger.info("Trying port = " + lowPort);
//			try(ServerSocket s = new ServerSocket(lowPort);) {
//				classLogger.info("Success with port = " + lowPort);
//				// no error, found an open port, we can stop
//				found = true;
//				s.close();
//				break;
//			} catch (Exception ex) {
//				// do nothing
//				classLogger.info("Port " + lowPort + " Failed. " + ex.getMessage());
//				found = false;
////				logger.error(Constants.STACKTRACE, ex);
//			}
//		}
//
//		// if we found a port, return that port
//		if (found) {
//			return lowPort + "";
//		}
//		
//		// no available ports in the range, either config is bad or something else is messed up
//		// just throw an exception
//		throw new IllegalArgumentException("Could not find available port to connect to");
//	}

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
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
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
	 * Write a task to a file using the specified separator
	 * 
	 * @param fileLocation
	 * @param task
	 * @param seperator
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, ITask task, String seperator) {
		Map<String, SemossDataType> typesMap = TaskUtility.getTypesMapFromTask(task);
		return Utility.writeResultToFile(fileLocation, task, typesMap, seperator);
	}
	
	/**
	 * 
	 * @param fileLocation
	 * @param it
	 * @param typesMap
	 * @param seperator
	 * @param exportProcessors
	 * @return
	 */
	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it,
			Map<String, SemossDataType> typesMap, String seperator, IStringExportProcessor... exportProcessors) {
		long start = System.currentTimeMillis();

		// make sure file is empty so we are only inserting the new values
		File f = new File(fileLocation);
		if (f.exists()) {
			classLogger.debug("File currently exists.. deleting file");
			f.delete();
		}
		try {
			f.createNewFile();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
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
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
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
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
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
			classLogger.error(Constants.STACKTRACE, ioe);
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (osw != null) {
					osw.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (fos != null) {
					fos.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
		}

		long end = System.currentTimeMillis();
		classLogger.info("Time to output file = " + (end - start) + " ms. File written to:" + Utility.normalizePath(fileLocation));

		return f;
	}
	
	/**
	 * 
	 * @param fileLocation
	 * @param it
	 * @return
	 */
	public static File writeResultToJson(String fileLocation, Iterator<IHeadersDataRow> it) {
		return writeResultToJson(fileLocation, it, null);
	}
	
	/**
	 * 
	 * @param fileLocation
	 * @param it
	 * @param typesMap
	 * @param exportProcessors
	 * @return
	 */
	public static File writeResultToJson(String fileLocation, Iterator<IHeadersDataRow> it,
			Map<String, SemossDataType> typesMap, IStringExportProcessor... exportProcessors) {
		long start = System.currentTimeMillis();

		// make sure file is empty so we are only inserting the new values
		File f = new File(fileLocation);
		if (f.exists()) {
			classLogger.debug("File currently exists.. deleting file");
			f.delete();
		}
		try {
			f.createNewFile();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}

		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		BufferedWriter bufferedWriter = null;

		try {
			fos = new FileOutputStream(f);
			osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
			bufferedWriter = new BufferedWriter(osw);
			bufferedWriter.write("[");
			// store some variables and just reset
			// should be faster than creating new ones each time
			int i = 0;
			int size = 0;
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
				for (; i < size; i++) {
					if (typesMap == null) {
						typesArr[i] = SemossDataType.STRING;
					} else {
						typesArr[i] = typesMap.get(headers[i]);
						if (typesArr[i] == null) {
							typesArr[i] = SemossDataType.STRING;
						}
					}
				}

				// generate the data row
				Object[] dataRow = row.getValues();
				i = 0;
				JSONObject json = new JSONObject();
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if(dataRow[i] == null 
								|| (dataRow[i] instanceof Double && Double.isNaN((double) dataRow[i]))
								|| (dataRow[i] instanceof Double && Double.isInfinite((double) dataRow[i]))
								){
							json.put(headers[i], JSONObject.NULL);
						} else {
							json.put(headers[i], dataRow[i]);
						}
					}
				}
				bufferedWriter.write(json.toString());
				bufferedWriter.flush();
			}

			// now loop through all the data
			while (it.hasNext()) {
				IHeadersDataRow row = it.next();
				// generate the data row
				Object[] dataRow = row.getValues();
				i = 0;
				
				JSONObject json = new JSONObject();
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if(dataRow[i] == null 
								|| (dataRow[i] instanceof Double && Double.isNaN((double) dataRow[i]))
								|| (dataRow[i] instanceof Double && Double.isInfinite((double) dataRow[i]))
								){
							json.put(headers[i], JSONObject.NULL);
						} else {
							json.put(headers[i], dataRow[i]);
						}
					}
				}
				// write row to file
				bufferedWriter.write(",");
				bufferedWriter.write(json.toString());
				bufferedWriter.flush();
			}

			// close the array
			bufferedWriter.write("]");
			bufferedWriter.flush();
			
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (osw != null) {
					osw.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
			try {
				if (fos != null) {
					fos.close();
				}
			} catch (IOException ioe) {
				classLogger.error(Constants.STACKTRACE, ioe);
			}
		}

		long end = System.currentTimeMillis();
		classLogger.info("Time to output file = " + (end - start) + " ms. File written to:" + Utility.normalizePath(fileLocation));

		return f;
	}
	

	public static JSONArray writeResultToJsonObject(Iterator<IHeadersDataRow> it,
			Map<String, SemossDataType> typesMap, IStringExportProcessor... exportProcessors) {
		long start = System.currentTimeMillis();
		 
		JSONArray jsonArray = new JSONArray();
		try {
			// store some variables and just reset
			// should be faster than creating new ones each time
			int i = 0;
			int size = 0;
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
				for (; i < size; i++) {
					if (typesMap == null) {
						typesArr[i] = SemossDataType.STRING;
					} else {
						typesArr[i] = typesMap.get(headers[i]);
						if (typesArr[i] == null) {
							typesArr[i] = SemossDataType.STRING;
						}
					}
				}
	
				// generate the data row
				Object[] dataRow = row.getValues();
				i = 0;
				JSONObject json = new JSONObject();
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if(dataRow[i] == null) {
							json.put(headers[i], "null");
						} else {
							json.put(headers[i], dataRow[i]);
						}
					}
				}
				jsonArray.put(json);
			}
		
			// now loop through all the data
			while (it.hasNext()) {
				IHeadersDataRow row = it.next();
				// generate the data row
				Object[] dataRow = row.getValues();
				i = 0;
				
				JSONObject json = new JSONObject();
				for (; i < size; i++) {
					if (typesArr[i] == SemossDataType.STRING) {
						// use empty quotes
						if(dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if(dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if(exportProcessors != null) {
								for(IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if(dataRow[i] == null) {
							json.put(headers[i], "null");
						} else {
							json.put(headers[i], dataRow[i]);
						}
					}
				}
				// write row to file
				jsonArray.put(json);
			}
		} catch (Exception e) {
			throw e;
		}
		
		long end = System.currentTimeMillis();
		classLogger.info("Time to output file = " + (end - start) + " ms.");
		return jsonArray;
	}

	public static String encodeURIComponent(String s) {
		try {
			s = URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20")
					.replace("!", "\\%21")
					.replace("'", "\\%27")
					.replace("(", "\\%28")
					.replace(")", "\\%29")
					.replace("~", "\\%7E")
					;
		} catch (UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return s;
	}

	public static String decodeURIComponent(String s) {
		try {
			String newS = s.replaceAll("\\%20", "+")
					.replaceAll("\\%21", "!")
					.replaceAll("\\%27", "'")
					.replaceAll("\\%28", "(")
					.replaceAll("\\%29", ")")
					.replaceAll("\\%7E", "~")
					;
			s = URLDecoder.decode(newS, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return s;
	}

	// ensure no CRLF injection into logs for forging records
	public static String cleanLogString(String message) {
		if(message == null) {
			return message;
		}
		message = message.replace('\n', '_').replace('\r', '_').replace('\t', '_');

		if(Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.LOG_ENCODING) + "")) {
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

	
	/**
	 * 
	 * @param stringToNormalize
	 * @return
	 */
	public static String normalizePath(String stringToNormalize) {
		if(stringToNormalize == null ) {
			return stringToNormalize;
		}
		//replacing \\ with /
		stringToNormalize=stringToNormalize.replace("\\", "/");
		//ensuring no double //
		while(stringToNormalize.contains("//")){
			stringToNormalize=stringToNormalize.replace("//", "/");
		}

		String normalizedString = Normalizer.normalize(stringToNormalize, Form.NFKC);
		normalizedString = FilenameUtils.normalize(normalizedString);
		if (normalizedString == null) {
			classLogger.error("File path is null");
			throw new IllegalArgumentException("The filepath passed in is invalid");
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
				classLogger.info("Unable to read properties file: " + Utility.normalizePath(filePath));
				classLogger.error(Constants.STACKTRACE, ioe);
			} finally {
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException ioe) {
						classLogger.error(Constants.STACKTRACE, ioe);
					}
				}
			}
		}
		return retProp;
	}
	
	/**
	 * 
	 * @param propertiesAsString
	 * @return
	 */
	public static Properties loadPropertiesString(String propertiesAsString) {
	    Properties retProp = new Properties();
	    try(StringReader is = new StringReader(propertiesAsString)) {
	    	try {
				retProp.load(is);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
	    }
	    return retProp;
	}
	
	/**
	 * Determine if for this instance only the admin can add a project
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectAdd() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_PROJECT_ADD);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can delete a project
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectDelete() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_PROJECT_DELETE);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can add/set project access
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectAddAccess() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_PROJECT_ADD_ACCESS);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can add/set insight access
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightAddAccess() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_INSIGHT_ADD_ACCESS);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can set a project public
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectSetPublic() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_PROJECT_SET_PUBLIC);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can set the project discoverable
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectSetDiscoverable() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_PROJECT_SET_DISCOVERABLE);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can add a database
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbAdd() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_DB_ADD);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can share insight
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightShare() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_INSIGHT_SHARE);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can delete a database
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbDelete() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_DB_DELETE);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can add/set database access
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbAddAccess() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_DB_ADD_ACCESS);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can set a database public
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbSetPublic() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_DB_SET_PUBLIC);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can set a database discoverable 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbSetDiscoverable() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_DB_SET_DISCOVERABLE);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Determine if for this instance only the admin can set an insight public
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightSetPublic() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_INSIGHT_SET_PUBLIC);
		if(boolString == null) {
			// default false
			return false;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * Get a comma separated list of widget ids to filter on
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationPipelineLandingFilter() {
		String filterList = DIHelper.getInstance().getProperty(Constants.PIPELINE_LANDING_FILTER);
		if(filterList == null || (filterList=filterList.trim()).isEmpty()) {
			// default null
			return null;
		}
		
		return filterList.split(",");
	}
	
	/**
	 * Get a comma separated list of widget ids to filter on
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationPipelineSourceFilter() {
		String filterList = DIHelper.getInstance().getProperty(Constants.PIPELINE_SOURCE_FILTER);
		if(filterList == null || (filterList=filterList.trim()).isEmpty()) {
			// default null
			return null;
		}
		
		return filterList.split(",");
	}
	
	/**
	 * Get a comma separated list of widget ids to filter on
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationWidgetTabShareExportList() {
		String filterList = DIHelper.getInstance().getProperty(Constants.WIDGET_TAB_SHARE_EXPORT_LIST);
		if(filterList == null || (filterList=filterList.trim()).isEmpty()) {
			// default null
			return null;
		} 
		
		return filterList.split(",");
	}
	
	/**
	 * Determine if on the application we should cache insights or not
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
	 * Determine amount of time to cache insights by default
	 * @return
	 */
	public static int getApplicationCacheInsightMinutes() {
		String cacheSetting = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHT_CACHE_MINUTES);
		if(cacheSetting == null) {
			// default is no limit 
			return -1;
		}
		
		return Integer.parseInt(cacheSetting);
	}
	
	/**
	 * Determine cron schedule for the cache existence
	 * @return
	 */
	public static String getApplicationCacheCron() {
		String cacheSetting = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHT_CACHE_CRON);
		if(cacheSetting == null) {
			// default is false
			return null;
		}
		
		cacheSetting = cacheSetting.trim();
		
		if (!CronExpression.isValidExpression(cacheSetting)) {
			classLogger.error("Application DEFAULT_INSIGHT_CACHE_CRON value of '" + cacheSetting + "' is not a valid cron expression");
			return null;
		}
		
		return cacheSetting;
	}
	
	/**
	 * Determine if the cache should be encrypted by default or not
	 * @return
	 */
	public static boolean getApplicationCacheEncrypt() {
		String cacheSetting = DIHelper.getInstance().getProperty(Constants.DEFAULT_INSIGHT_CACHE_ENCRYPT);
		if(cacheSetting == null) {
			// default is false
			return false;
		}
		
		return Boolean.parseBoolean(cacheSetting);
	}
	
	/**
	 * Determine if HashiCorp Vault is enabled for secrets
	 * @return
	 */
	public static boolean isSecretsStoreEnabled() {
		String hashiCorpEnabled = DIHelper.getInstance().getProperty(Constants.SECRET_STORE_ENABLED);
		if(hashiCorpEnabled == null) {
			// default configuration is false
			return false;
		}
		
		return Boolean.parseBoolean(hashiCorpEnabled);
	}
	
	/**
	 * Determine if virus scanner enabled
	 * @return
	 */
	public static boolean isVirusScanningEnabled() {
		String virusScanning = DIHelper.getInstance().getProperty(Constants.VIRUS_SCANNING_ENABLED);
		if(virusScanning == null) {
			// default configuration is false
			return false;
		}
		
		return Boolean.parseBoolean(virusScanning);
	}
	
	public static boolean isVirusScanningDisabled() {
		return !isVirusScanningEnabled();
	}
	
	/**
	 * Determine if user tracking enabled
	 * @return
	 */
	public static boolean isUserTrackingEnabled() {
		String userTracking = DIHelper.getInstance().getProperty(Constants.USER_TRACKING_ENABLED);
		if(userTracking == null) {
			// default configuration is false
			return false;
		}
		
		return Boolean.parseBoolean(userTracking);
	}
	
	/**
	 * Determine if model inference logs db is enabled
	 * @return
	 */
	public static boolean isModelInferenceLogsEnabled() {
		String modelInferenceLogs = DIHelper.getInstance().getProperty(Constants.MODEL_INFERENCE_LOGS_ENABLED);
		if(modelInferenceLogs == null) {
			// default configuration is false
			return false;
		}
		
		return Boolean.parseBoolean(modelInferenceLogs);
	}
	
	/**
	 * Determine if user tracking enabled
	 * @return
	 */
	public static boolean schedulerForceDisable() {
		String schedulerForceDisable  = DIHelper.getInstance().getProperty(Constants.SCHEDULER_FORCE_DISABLE);
		if(schedulerForceDisable == null) {
			// default configuration is false
			return false;
		}
		
		return Boolean.parseBoolean(schedulerForceDisable);
	}
	
	public static boolean isUserTrackingDisabled() {
		return !isUserTrackingEnabled();
	}
	
	public static String getUserTrackingMethod() {
		return DIHelper.getInstance().getProperty(Constants.USER_TRACKING_METHOD);
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getDefaultTerminalMode() {
		String terminalMode  = DIHelper.getInstance().getProperty(Constants.TERMINAL_MODE);
		if(terminalMode == null || (terminalMode=terminalMode.trim()).isEmpty()) {
			// default configuration is false
			return "cmd";
		}
		
		List<String> valid = new ArrayList<>();
		valid.add("cmd");
		valid.add("powershell");
		
		if(!valid.contains(terminalMode) && !valid.contains(terminalMode.toLowerCase())) {
			classLogger.warn("Invalid terminal mode = " + terminalMode + ". Switching to cmd for windows and bash for mac/linux.");
		}
		
		return terminalMode;
	}
	
	/**
	 * Determine if We need to show Welcome Dialog on Application Load
	 * @return
	 */
	public static boolean getWelcomeBannerOption() {
		String welcomeDialog = DIHelper.getInstance().getProperty(Constants.SHOW_WELCOME_BANNER);
		if(welcomeDialog == null) {
			// default option is true
			return true;
		}
		
		return Boolean.parseBoolean(welcomeDialog);
	}
	
	/**
	 * Get the application time zone
	 * 
	 * Switch to using Utility.getApplicationZoneId()
	 * 
	 * @return
	 */
	@Deprecated
	public static String getApplicationTimeZoneId() {
		String timeZone = DIHelper.getInstance().getProperty(Constants.DEFAULT_TIME_ZONE);
		if(timeZone == null || (timeZone=timeZone.trim()).isEmpty()) {
			// default to ET
			return "America/New_York";
		}
		
		return timeZone.trim();
	}
	
	/**
	 * Get the application zone id
	 * @return
	 */
	public static String getApplicationZoneId() {
		String timeZone = DIHelper.getInstance().getProperty(Constants.DEFAULT_TIME_ZONE);
		if(timeZone == null || (timeZone=timeZone.trim()).isEmpty()) {
			// default to system location
			return ZoneId.systemDefault().getId();
		}
		
		return timeZone.trim();
	}
	
	public static ZoneId getApplicationZoneIdObj() {
		return ZoneId.of(getApplicationZoneId());

	}
	
	/**
	 * 
	 * @return
	 */
	public static Boolean getApplicationAdminOnlyCreateAPIUser() {
		String boolString = DIHelper.getInstance().getProperty(Constants.ADMIN_ONLY_CREATE_API_USER);
		if(boolString == null || (boolString=boolString.trim()).isEmpty()) {
			// default to true
			return true;
		}
		
		return Boolean.parseBoolean(boolString);
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getSameSiteCookieValue() {
		String sameSiteString = DIHelper.getInstance().getProperty(Constants.SAMESITE_COOKIE);
		if(sameSiteString == null || (sameSiteString=sameSiteString.trim()).isEmpty()) {
			return "none";
		}
		
		if(!sameSiteString.equalsIgnoreCase("strict") && 
				!sameSiteString.equalsIgnoreCase("lax") &&
				!sameSiteString.equalsIgnoreCase("none")) {
			classLogger.warn("Invalid samesite cookie option = '" + sameSiteString +"'. Must be 'strict', 'lax', or 'none'");
			classLogger.warn("Default to samesite cookie option 'none'");
			return "none";
		}
		
		return sameSiteString;
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getApplicationBaseUrl() {
		// derived from the social.properties redirect value
		try {
			String redirectUrlStr = SocialPropertiesUtil.getInstance().getProperty("redirect");
			URL redirectUrl = new URL(redirectUrlStr);
			String protocol = redirectUrl.getProtocol();
			int port = redirectUrl.getPort();
			String host = redirectUrl.getHost();
			if(port > 0) {
				return protocol + "://" + host + ":" + port; 
			} else {
				return protocol + "://" + host;
			}
		} catch(MalformedURLException e) {
			classLogger.warn("Invalid redirect URL in social.properties for redirect");
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	public static String getApplicationContextPath() {
		String contextPath = (String) DIHelper.getInstance().getLocalProp(Constants.CONTEXT_PATH_KEY);
		if(contextPath == null || (contextPath=contextPath.trim()).isEmpty()) {
			return null;
		}
		
		if(contextPath.startsWith("/")) {
			contextPath = contextPath.substring(1);
		}
		if(contextPath.endsWith("/")) {
			contextPath = contextPath.substring(0, contextPath.length()-1);
		}
		return contextPath;
	}
	
	public static String getApplicationOptionalRoutePath() {
		String route = (String) DIHelper.getInstance().getLocalProp(Constants.MONOLITH_ROUTE);
		if(route == null) {
			Map<String, String> envMap = System.getenv();
			if (envMap.containsKey(Constants.MONOLITH_ROUTE)) {
				route = envMap.get(Constants.MONOLITH_ROUTE);
			}
		}
		if(route == null || (route=route.trim()).isEmpty()) {
			return null;
		}
		
		if(route.startsWith("/")) {
			route = route.substring(1);
		}
		if(route.endsWith("/")) {
			route = route.substring(0, route.length()-1);
		}
		return route;
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getApplicationUrl() {
		// derived from the social.properties redirect value
		String baseUrl = getApplicationBaseUrl();
		String optionalRoute = getApplicationOptionalRoutePath();
		String contextPath = getApplicationContextPath();
		String url = baseUrl;
		if(optionalRoute != null && !(optionalRoute=optionalRoute.trim()).isEmpty()) {
			url=url+"/"+optionalRoute;
		}
		url=url+"/"+contextPath;
		return url;
	}
	
	/**
	 * 
	 * @return
	 */
	public static String getApplicationRouteAndContextPath() {
		String optionalRoute = getApplicationOptionalRoutePath();
		String contextPath = getApplicationContextPath();
		String routeAndContext = "/"+contextPath;
		if(optionalRoute != null && !(optionalRoute=optionalRoute.trim()).isEmpty()) {
			routeAndContext = "/"+optionalRoute + routeAndContext;
		}
		return routeAndContext;
	}
	
	/**
	 * Default value is public_home
	 * @return
	 */
	public static String getPublicHomeFolder() {
		String publicHomeFolder = "public_home";
		if(DIHelper.getInstance().getProperty(Settings.PUBLIC_HOME) != null) {
			publicHomeFolder = DIHelper.getInstance().getProperty(Settings.PUBLIC_HOME);
		}
		// assume public home is clean for lower paths
		if(publicHomeFolder.startsWith("/")) {
			publicHomeFolder = publicHomeFolder.substring(1);
		}
		if(publicHomeFolder.endsWith("/")) {
			publicHomeFolder = publicHomeFolder.substring(0, publicHomeFolder.length()-1);
		}
		return publicHomeFolder;
	}
	
	/**
	 * 
	 * @param urlString
	 * @param filePath
	 */
	public static void copyURLtoFile(String urlString, String filePath) {
		try(PrintWriter out = new PrintWriter(filePath)){
			URL url = new URL(urlString);
			URLConnection conn = url.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;
			
			// write file
			while ((inputLine = in.readLine()) != null) {
				out.write(inputLine + System.getProperty("line.separator"));
			}
			out.close();
			in.close();
		} catch (MalformedURLException mue) {
			classLogger.error(Constants.STACKTRACE, mue);
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}
	}

	public static Map<String, Class> loadReactors(String folder, String key) {
		HashMap<String, Class> thisMap = new HashMap<>();
		
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
				 classLogger.debug("Project specific reactors are disabled");
				 return thisMap;
			 }
		}
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
			classesFolder = Utility.normalizePath(classesFolder.replaceAll("\\\\", "/"));

			File file = new File(classesFolder);
			if (file.exists()) {
				// loads a class and tried to change the package of the class on the fly
				// CtClass clazz = pool.get("prerna.test.CPTest");

				classLogger.error("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				Map<String, Class> reactors = new HashMap<>();

				ScanResult sr = new ClassGraph()
						.overrideClasspath((new File(classesFolder).toURI().toURL()))
						.whitelistPackages(packages).scan();
				
				String[] subclassSearch = new String[] {
						AbstractReactor.class.getName(),
						prerna.sablecc2.reactor.AbstractReactor.class.getName(),
					};
				
				for(String subclass : subclassSearch) {
					ClassInfoList classes = sr.getSubclasses(subclass);

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
								if (newInstance instanceof AbstractReactor) {
									thisMap.put(name.toUpperCase().replaceAll("REACTOR", ""), newClass);
								} else if(newInstance instanceof prerna.sablecc2.reactor.AbstractReactor) {
									thisMap.put(name.toUpperCase().replaceAll("REACTOR", ""), newClass);
								}
							} catch (NotFoundException nfe) {
								classLogger.error(Constants.STACKTRACE, nfe);
							} catch (CannotCompileException cce) {
								classLogger.error(Constants.STACKTRACE, cce);
							}

							// once the new instance has been done.. it has been injected into heap.. after
							// this anyone can access it.
							// no way to remove this class from heap
							// has to be garbage collected as it moves
						}
					}
				}
			}
		} catch (Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}

		return thisMap;
	}

	public static Map<String, Class<IReactor>> loadReactors(String folder, SemossClassloader customClassLoader) {
		return loadReactors(folder, customClassLoader, "classes");
	}

	// loads classes through this specific class loader for the insight
	public static Map<String, Class<IReactor>> loadReactors(String folder, SemossClassloader customClassLoader, String outputFolder) {
		Map<String, Class<IReactor>> reactorMap = new HashMap<>();
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
				 classLogger.debug("Project specific reactors are disabled");
				 return reactorMap;
			 }
		}
		try {
			// the main folder to add here is
			// basefolder/db/insightfolder/classes 
			String classesFolder = folder + "/" + outputFolder;

			classesFolder = classesFolder.replaceAll("\\\\", "/");
			customClassLoader.setFolder(classesFolder);

			File file = new File(classesFolder);
			if (file.exists()) {
				classLogger.info("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph()
						.overrideClasspath((new File(classesFolder).toURI().toURL()))
						.enableClassInfo()
						.whitelistPackages(packages)
						.scan();
				
				// find everything implementing IReactor
				// get implementing classes doesn't seem to work when overriding the classpath
				// likely because the base semoss classes are not in the scope of the ClassGraph object
				ClassInfoList classes = sr.getAllClasses(); 
				for(int classIndex = 0;classIndex < classes.size();classIndex++) {
					ClassInfo classObject = classes.get(classIndex);
					String className = classObject.getName();

					if(!classObject.isInterface() 
							&& !classObject.isAbstract() 
							&& classObject.isPublic() 
							&& isValidReactor(classObject)) {
						Class<IReactor> actualClass = (Class<IReactor>) customClassLoader.loadClass(className);
					
						String reactorName = classes.get(classIndex).getSimpleName();
						final String REACTOR_KEY = "REACTOR";
						if(reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
							reactorName = reactorName.substring(0, reactorName.length()-REACTOR_KEY.length());
						}
						
						reactorMap.put(reactorName.toUpperCase(), actualClass);
					}
				}
			}
		} catch (Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}

		return reactorMap;
	}
	
	public static boolean isValidReactor(ClassInfo classObject) {
		String className = classObject.getName();
		if(className.equals(AbstractRFrameReactor.class.getName())
				|| className.equals(AbstractPyFrameReactor.class.getName())
				|| className.equals(AbstractFrameReactor.class.getName())
				|| className.equals(AbstractReactor.class.getName())
				|| className.equals(IReactor.class.getName())
				|| className.equals(prerna.sablecc2.reactor.AbstractReactor.class.getName())
				) {
			return true;
		}
		if(classObject.implementsInterface(IReactor.class.getName())) {
			return true;
		}
		
		ClassInfo superClass = classObject.getSuperclass();
		if(superClass == null) {
			return false;
		}
		
		return isValidReactor(superClass);
	}

	// loads classes through this specific class loader for the insight
	public static Map<String, Class<IReactor>> loadReactorsFromPom(String folder, JarClassLoader cl, String outputFolder) {
		Map<String, Class<IReactor>> reactors = new HashMap<>();
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
				 classLogger.debug("Project specific reactors are disabled");
				 return reactors;
			 }
		}
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
			cl.add(classesFolder);

			File file = new File(classesFolder);
			if (file.exists()) {
				// loads a class and tried to change the package of the class on the fly
				// CtClass clazz = pool.get("prerna.test.CPTest");

				classLogger.error("Loading reactors from >> " + classesFolder);

				Map<String, List<String>> dirs = GitAssetUtils.browse(classesFolder, classesFolder);
				List<String> dirList = dirs.get("DIR_LIST");

				// get the directories before scanning
				String[] packages = new String[dirList.size()];
				for (int dirIndex = 0; dirIndex < dirList.size(); dirIndex++) {
					packages[dirIndex] = dirList.get(dirIndex);
				}

				ScanResult sr = new ClassGraph()
						// .whitelistPackages("prerna")
						.overrideClasspath((new File(classesFolder).toURI().toURL()))
						// .enableAllInfo()
						// .enableClassInfo()
						.whitelistPackages(packages)
						.scan();
				
				String[] subclassSearch = new String[] {
						AbstractReactor.class.getName(),
						prerna.sablecc2.reactor.AbstractReactor.class.getName(),
					};
				
				for(String sublcass : subclassSearch) {
					ClassInfoList classes = sr.getSubclasses(sublcass);
					// add the path to the insight classes so only this guy can load it
					pool.insertClassPath(classesFolder);
	
					for (int classIndex = 0; classIndex < classes.size(); classIndex++) {
						// this will load the reactor with everything
						JclObjectFactory factory = JclObjectFactory.getInstance();
	
						  //Create object of loaded class
						Object loadedObject = factory.create(cl, classes.get(classIndex).getName());
	
						String reactorName = classes.get(classIndex).getSimpleName();
						final String REACTOR_KEY = "REACTOR";
						if(reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
							reactorName = reactorName.substring(0, reactorName.length()-REACTOR_KEY.length());
						}
	
						reactors.put(reactorName.toUpperCase(), (Class<IReactor>) loadedObject.getClass());
					}
				}
			}
		} catch (Exception ex) {
			classLogger.error(Constants.STACKTRACE, ex);
		}

		return reactors;
	}
	
	/**
	 * Load reactors directly from a compiled jar(s)
	 * @param urls
	 * @return
	 */
	public static Map<String, Class<IReactor>> loadReactorsFromJars(URL[] urls) {
		URLClassLoader cl = null;
		Map<String, Class<IReactor>> reactorsMap = new HashMap<>();
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			if(Boolean.parseBoolean(disable_terminal)) {
				classLogger.debug("Project specific reactors are disabled");
				return reactorsMap;
			};
		}
		try {
			cl = new URLClassLoader(urls);
			JarClassLoader jcl = new JarClassLoader(cl);
			
			// scan all abstract reactors
			ScanResult sr = new ClassGraph()
					.overrideClasspath((Object[]) urls)
					.enableClassInfo()
					.scan();
			
			// find everything implementing IReactor
			// get implementing classes doesn't seem to work when overriding the classpath
			// likely because the base semoss classes are not in the scope of the ClassGraph object
			ClassInfoList classes = sr.getAllClasses(); 
			for(int classIndex = 0;classIndex < classes.size();classIndex++) {
				ClassInfo classObject = classes.get(classIndex);
				String className = classObject.getName();
//				System.out.println(className);

				if(!classObject.isInterface() 
						&& !classObject.isAbstract() 
						&& classObject.isPublic() 
						&& isValidReactor(classObject)) {
					Class<IReactor> actualClass = jcl.loadClass(className);
					
					String reactorName = classes.get(classIndex).getSimpleName();
					final String REACTOR_KEY = "REACTOR";
					if(reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
						reactorName = reactorName.substring(0, reactorName.length()-REACTOR_KEY.length());
					}
					
					reactorsMap.put(reactorName.toUpperCase(), actualClass);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(cl != null) {
				try {
					cl.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return reactorsMap;
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
			classLogger.error(Constants.STACKTRACE, cnfe);
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
			classLogger.error(Constants.STACKTRACE, cnfe);
		}

		return envClassPath.toString();
	}
	
	public static Process startTCPServerChroot(String cp, String chrootDir, String insightFolder, String port) {
		// this basically starts a java process
		// the string is an identifier for this process
		Process thisProcess = null;
		if (cp == null) {
			cp = "fst-2.56.jar;jep-3.9.0.jar;log4j-1.2.17.jar;commons-io-2.4.jar;objenesis-2.5.1.jar;jackson-core-2.9.5.jar;javassist-3.20.0-GA.jar;netty-all-4.1.47.Final.jar;classes";
		}
		String specificPath = getCP(cp, insightFolder);
		try {
			String java = System.getenv(Constants.JAVA_HOME);
			if (java == null) {
				java = DIHelper.getInstance().getProperty(Constants.JAVA_HOME);
			}
			if(!java.endsWith("bin")) {
				//seems like for graal
				java = java + "/bin/java";
			} else {
				java = java + "/java";
			}
			// account for spaces in the path to java
			if (java.contains(" ")) {
				java = "\"" + java + "\"";
			}
			// change the \\
			java = java.replace("\\", "/");

			String jep = DIHelper.getInstance().getProperty(Constants.LD_LIBRARY_PATH);
			if (jep == null) {
				jep = System.getenv(Constants.LD_LIBRARY_PATH);
			}
			// account for spaces in the path to jep
			if (jep.contains(" ")) {
				jep = "\"" + jep + "\"";
			}
			jep = jep.replace("\\", "/");

			String pyWorker = DIHelper.getInstance().getProperty(Constants.TCP_WORKER);
			if(pyWorker == null || (pyWorker=pyWorker.trim()).isEmpty()) {
				pyWorker = prerna.tcp.SocketServer.class.getName(); // "prerna.tcp.SocketServer";
			}
			String[] commands = null;
			if (port == null) {
				commands = new String[7];
			} else {
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
			// compose for memory
			String xms = DIHelper.getInstance().getProperty("Xms");
			String xmx = DIHelper.getInstance().getProperty("Xmx");
			
			String memory = "";
			if(xms != null && xmx != null)
				memory = "-Xms" + xms + " -Xmx" + xmx;
			
			commands[2] = memory + " -cp";

			//commands[2] = "-cp";
			commands[3] = specificPath;
			commands[4] = pyWorker;
			commands[5] = finalDir;
			commands[6] = DIHelper.getInstance().getRDFMapFileLocation();
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

			classLogger.debug("Trying to create file in .. " + finalDir);
			File file = new File(chrootDir + finalDir + "/init");
			file.createNewFile();
			classLogger.debug("Python start commands ... ");
			classLogger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// run it as a process
			// ProcessBuilder pb = new ProcessBuilder(commands);
			// ProcessBuilder pb = new
			// ProcessBuilder("c:/users/pkapaleeswaran/workspacej3/temp/mango.bat");
			// pb.command(commands);

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS && !(Strings.isNullOrEmpty(DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT)))){
				String ulimit = DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " +  ulimit + " && " + sb.toString() + "\"" };
			}

			String[] starterFile = writeStarterFile(commands, chrootDir, finalDir);
			ProcessBuilder pb = new ProcessBuilder(starterFile);
			pb.redirectError();
			classLogger.info("came out of the waiting for process");
			Process p = pb.start();

			try {
				// p.waitFor();
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error(Constants.STACKTRACE, ie);
			}
			classLogger.info("came out of the waiting for process");
			thisProcess = p;

			// System.out.println("Process started with .. " + p.exitValue());
			// thisProcess = Runtime.getRuntime().exec(java + " -cp " + cp + " " + className
			// + " " + argList);
			// thisProcess = Runtime.getRuntime().exec(java + " " + className + " " +
			// argList + " > c:/users/pkapaleeswaran/workspacej3/temp/java.run");
			// thisProcess = pb.start();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}

		return thisProcess;
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
			String java = System.getenv(Constants.JAVA_HOME);
			if (java == null) {
				java = DIHelper.getInstance().getProperty(Constants.JAVA_HOME);
			}
			if(!java.endsWith("bin")) {
				//seems like for graal
				java = java + "/bin/java";
			} else {
				java = java + "/java";
			}
			// account for spaces in the path to java
			if (java.contains(" ")) {
				java = "\"" + java + "\"";
			}
			// change the \\
			java = java.replace("\\", "/");

			String jep = DIHelper.getInstance().getProperty(Constants.LD_LIBRARY_PATH);
			if (jep == null) {
				jep = System.getenv(Constants.LD_LIBRARY_PATH);
			}
			// account for spaces in the path to jep
			if (jep.contains(" ")) {
				jep = "\"" + jep + "\"";
			}
			jep = jep.replace("\\", "/");

			String pyWorker = DIHelper.getInstance().getProperty(Constants.TCP_WORKER);
			if(pyWorker == null || (pyWorker=pyWorker.trim()).isEmpty()) {
				pyWorker = prerna.tcp.SocketServer.class.getName(); // "prerna.tcp.SocketServer";
			}
			String[] commands = null;
			if (port == null) {
				commands = new String[7];
			} else {
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
			// compose for memory
			String xms = DIHelper.getInstance().getProperty("Xms");
			String xmx = DIHelper.getInstance().getProperty("Xmx");
			
			String memory = "";
			if(xms != null && xmx != null)
				memory = "-Xms" + xms + " -Xmx" + xmx;
			
			commands[2] = memory + " -cp";

			commands[3] = specificPath;
			commands[4] = pyWorker;
			commands[5] = finalDir;
			commands[6] = DIHelper.getInstance().getRDFMapFileLocation();
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

			classLogger.debug("Trying to create file in .. " + finalDir);
			File file = new File(finalDir + "/init");
			file.createNewFile();
			classLogger.debug("Python start commands ... ");
			classLogger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// run it as a process
			// ProcessBuilder pb = new ProcessBuilder(commands);
			// ProcessBuilder pb = new
			// ProcessBuilder("c:/users/pkapaleeswaran/workspacej3/temp/mango.bat");
			// pb.command(commands);

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS && !(Strings.isNullOrEmpty(DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT)))){
				String ulimit = DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT);
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
			Process p = pb.start();
			try {
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error(Constants.STACKTRACE, ie);
			}
			classLogger.info("came out of the waiting for process");
			thisProcess = p;

			// System.out.println("Process started with .. " + p.exitValue());
			// thisProcess = Runtime.getRuntime().exec(java + " -cp " + cp + " " + className
			// + " " + argList);
			// thisProcess = Runtime.getRuntime().exec(java + " " + className + " " +
			// argList + " > c:/users/pkapaleeswaran/workspacej3/temp/java.run");
			// thisProcess = pb.start();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}

		return thisProcess;
	}
	
	public static Object [] startTCPServerNativePyChroot(String chrootDir, String insightFolder, String port, String timeout, String loggerLevel ) {
		//chroot dir is usually at /opt/kunal__abc123123 - after which is the full os
		// this basically starts a java process
		// the string is an identifier for this process
		// do I need this insight folder anymore ?
		
		// py gaas_tcp_socket_server.py 86 1 py_base_directory insight_folder_dir
		// C:/Python/Python310/python.exe C:/Users/pkapaleeswaran/workspacej3/SemossDev/py/gaas_tcp_socket_server.py 9999 1 . c:/temp
		String prefix = "";
		Process thisProcess = null;
		String finalDir = insightFolder.replace("\\", "/");

		try {
			String py = System.getenv(Settings.PYTHONHOME);
			if(py == null) {
				py = DIHelper.getInstance().getProperty(Settings.PYTHONHOME);
			}
			if(py == null) {
				System.getenv(Settings.PY_HOME);
			}
			if (py == null) {
				py = DIHelper.getInstance().getProperty(Settings.PY_HOME);
			}
			if(py == null) {
				throw new NullPointerException("Must define python home");
			}
			
			if (SystemUtils.IS_OS_WINDOWS) {
				py = py + "/python.exe";
			} else {
				py = py + "/bin/python3";
			}
			
			py = py.replace("\\", "/");

			String pyBase = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
			pyBase = pyBase.replace("\\", "/");
			String gaasServer = pyBase + "/gaas_tcp_socket_server.py";

			prefix = Utility.getRandomString(5);
			prefix = "p_"+ prefix;
			
			String outputFile =chrootDir + finalDir + "/console.txt";
			
			//String timeout = "15";
//			if(otherProps!= null && otherProps.length > 0)
//				timeout = otherProps[0];
			
		//	String[] commands = new String[] {"fakechroot", "fakeroot", "chroot","--userspec=1001:1001" , chrootDir, py, gaasServer, port, "1", pyBase, finalDir, prefix, timeout};

			String[] commands = new String[] {"fakechroot", "fakeroot", "chroot","--userspec=1001:1001" , chrootDir, py, gaasServer, "--port", port, "--max_count", "1", "--py_folder", pyBase, "--insight_folder", finalDir, "--prefix", prefix, "--timeout", timeout, "--logger_level" , loggerLevel};

			// need to make sure we are not windows cause ulimit will not work
			if (!SystemUtils.IS_OS_WINDOWS && !(Strings.isNullOrEmpty(DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT)))){
				String ulimit = DIHelper.getInstance().getProperty(Constants.ULIMIT_R_MEM_LIMIT);
				StringBuilder sb = new StringBuilder();
				for (String str : commands) {
					sb.append(str).append(" ");
				}
				sb.substring(0, sb.length() - 1);
				commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " +  ulimit + " && " + sb.toString() + "\"" };
			}
			
			// do I need this ?
			//String[] starterFile = writeStarterFile(commands, finalDir);
			ProcessBuilder pb = new ProcessBuilder(commands);
			ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(new File(outputFile));
			pb.redirectError(redirector);
			pb.redirectOutput(redirector);
			Process p = pb.start();
			try {
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error(Constants.STACKTRACE, ie);
			}
			classLogger.info("came out of the waiting for process");
			thisProcess = p;

			// System.out.println("Process started with .. " + p.exitValue());
			// thisProcess = Runtime.getRuntime().exec(java + " -cp " + cp + " " + className
			// + " " + argList);
			// thisProcess = Runtime.getRuntime().exec(java + " " + className + " " +
			// argList + " > c:/users/pkapaleeswaran/workspacej3/temp/java.run");
			// thisProcess = pb.start();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}

		return new Object[] {thisProcess, prefix};
	}
	
	/**
	 * 
	 * @param pyClientClass
	 * @param port
	 * @return
	 */
	public static SocketClient startTCPClient(String pyClient, String port) {
		SocketClient tcpClient = null;
		try {
			tcpClient = (SocketClient) Class.forName(pyClient).newInstance();
			tcpClient.connect("127.0.0.1", Integer.parseInt(port), false);
			//nc.run(); - you cannot do this because then the client goes into listener mode
			Thread t = new Thread(tcpClient);
			t.start();
			while(!tcpClient.isReady())
			{
				synchronized(tcpClient)
				{
					try 
					{
						tcpClient.wait();
						classLogger.info("Setting the socket client ");
					} catch (InterruptedException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return tcpClient;
	}
	
	public static Process startRMIServer(String cp, String insightFolder, String port) {
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

			String pyWorker = DIHelper.getInstance().getProperty("RMI_WORKER");
			if(pyWorker == null)
				pyWorker = "prerna.rmi.Server";
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
			// compose for memory
			String xms = DIHelper.getInstance().getProperty("Xms");
			String xmx = DIHelper.getInstance().getProperty("Xmx");
			
			String memory = "";
			if(xms != null && xmx != null)
				memory = "-Xms" + xms + " -Xmx" + xmx;
			
			commands[2] = memory + " -cp";
			commands[3] = specificPath;
			commands[4] = pyWorker;
			commands[5] = finalDir;
			commands[6] = DIHelper.getInstance().getRDFMapFileLocation();
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

			classLogger.debug("Trying to create file in .. " + finalDir);
			File file = new File(finalDir + "/init");
			file.createNewFile();
			classLogger.debug("Python start commands ... ");
			classLogger.debug(new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(commands));

			// run it as a process
			// ProcessBuilder pb = new ProcessBuilder(commands);
			// ProcessBuilder pb = new
			// ProcessBuilder("c:/users/pkapaleeswaran/workspacej3/temp/mango.bat");
			// pb.command(commands);

			String[] starterFile = writeStarterFile(commands, finalDir);
			ProcessBuilder pb = new ProcessBuilder(starterFile);
			pb.redirectError();
			classLogger.info("came out of the waiting for process");
			Process p = pb.start();

			try {
				// p.waitFor();
				p.waitFor(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				classLogger.error(Constants.STACKTRACE, ie);
			}
			classLogger.info("came out of the waiting for process");
			thisProcess = p;

			// System.out.println("Process started with .. " + p.exitValue());
			// thisProcess = Runtime.getRuntime().exec(java + " -cp " + cp + " " + className
			// + " " + argList);
			// thisProcess = Runtime.getRuntime().exec(java + " " + className + " " +
			// argList + " > c:/users/pkapaleeswaran/workspacej3/temp/java.run");
			// thisProcess = pb.start();
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
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
			classLogger.error(Constants.STACKTRACE, fnfe);
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}
		
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty("ENABLE_BINDFS")) && osName.indexOf("win") < 0) { 
			commandsStarter =  new String[5];
			starter = dir + "/starter.sh";
			commandsStarter[0] = "fakechroot";
			commandsStarter[1] = "fakeroot";
			commandsStarter[2] = "chroot";
			commandsStarter[3] = "/bin/bash";
			commandsStarter[4] = starter;
		}

		
		return commandsStarter;
	}
	
	public static String[] writeStarterFile(String[] commands, String chrootDir, String dir) {
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
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE)) && osName.indexOf("win") < 0) { 
			commandsStarter =  new String[6];
			starter = dir + "/starter.sh";
			commandsStarter[0] = "fakechroot";
			commandsStarter[1] = "fakeroot";
			commandsStarter[2] = "chroot";
			commandsStarter[3] = chrootDir;
			commandsStarter[4] = "/bin/bash";
			commandsStarter[5] = starter;
			
			starter =chrootDir + dir + "/starter.sh";

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
			classLogger.error(Constants.STACKTRACE, fnfe);
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
		}
		

		
		return commandsStarter;
	}
	
	/**
	 * Write the log4j2.properties file for the Socket Server
	 * @param dir
	 */
	public static void writeLogConfigurationFile(String dir)
	{
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile);
			//property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", dir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * Write the log4j2.properties file for the Socket Server when chroot is enabled
	 * @param dir
	 * @param paramDir
	 */
	public static void writeLogConfigurationFile(String dir, String paramDir) {
		try {
			// read the file first
			dir = dir.replace("\\", "/");
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			File logFile = new File(baseFolder + "/py/log-config/log4j.properties");
			String logConfig = FileUtils.readFileToString(logFile);
			//property.filename = target/rolling/rollingtest.log
			logConfig = logConfig.replace("FILE_LOCATION", paramDir + "/output.log");
			File newLogFile = new File(dir + "/log4j2.properties");
			FileUtils.writeStringToFile(newLogFile, logConfig);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	private static boolean fileInRelativeHiddenDirectory(Path file, Path folder) {
		do {
			file = file.getParent();
			
			if (file == null) {
				break;
			}
			
			// this is not applicable to windows, but is the current behavior in
			// GitAssetUtils.listAssets(String, String, String, List<String>, List<String>)
			if (file.getFileName().toString().startsWith(".")) {
				return true;
			}
			
		} while (!file.equals(folder));
		
		return false;
	}
	
	private static boolean isJavaFile(Path path) {
		return FilenameUtils.getExtension(path.toString()).equals("java");
	}
	
	public static int compileJava(String folder, String classpath) {
		int status = -1;
		
		String javaFolder = folder + "/java";
		Path path = Paths.get(Utility.normalizePath(javaFolder));
		
		if (Files.isDirectory(path)) {
			classLogger.info("Compiling Java in Folder " + javaFolder);
			try (Stream<Path> p = Files.walk(path, FileVisitOption.FOLLOW_LINKS)) {
				List<File> files = p.filter(Files::isRegularFile)
					.map(Path::toAbsolutePath)
					.filter(Utility::isJavaFile)
					.filter(s -> !Utility.fileInRelativeHiddenDirectory(s, path))
					.map(Path::toFile)
					.collect(Collectors.toList());
				
				if (files.size() > 0) {
					status = compileJava(files, folder, classpath);
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			classLogger.info("Done compiling Java in Folder " + javaFolder);
		}
		
		return status;
	}
	
	private static int compileJava(List<File> files, String folder, String classpath) throws IOException {
		String outputFolder = folder + "/classes";	
		Files.createDirectories(Paths.get(Utility.normalizePath(outputFolder)));

		List<String> options = new ArrayList<>();
		options.add("-d");
		options.add(outputFolder);
		options.add("-cp");
		options.add(classpath);
		options.add("-proc:none");

		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if(compiler == null) {
			throw new NullPointerException("Could not find the java compiler");
		}
		StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(files);

		Path error = Paths.get(Utility.normalizePath(outputFolder), "compileerror.out");
		Files.deleteIfExists(error);
		
		try (OutputStream os = Files.newOutputStream(error)) {
			return compileJava(os, compiler, fileManager, diagnostics, options, compilationUnits);
		}	
	}
	
	private static int compileJava(OutputStream os, JavaCompiler compiler, StandardJavaFileManager fileManager, 
			DiagnosticCollector<JavaFileObject> diagnostics, List<String> options, 
			Iterable<? extends JavaFileObject> compilationUnits) throws IOException {

		try (PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)))) {
			boolean hold = compiler.getTask(
					pw, 
					fileManager, 
					diagnostics, 
					options, 
					null, 
					compilationUnits).call();

			pw.flush();
			logDiagnostics(diagnostics.getDiagnostics(), pw);
			
			return hold ? 0 : -1;
		} finally {
			fileManager.close();
		}
	}	

	private static void logDiagnostics(List<Diagnostic<? extends JavaFileObject>> diagnostics, PrintWriter pw) {
		for (Diagnostic<? extends JavaFileObject> x : diagnostics) {
			if(x.getSource() != null) {
				pw.println("[" + x.getKind().toString() + "] " + x.getSource().toUri() + ":" + x.getLineNumber()
				+ " - " + x.getMessage(Locale.getDefault()));
			} else {
				pw.println("[" + x.getKind().toString() + "] 'NO-SOURCE-MESSAGE':" + x.getLineNumber()
				+ " - " + x.getMessage(Locale.getDefault()));
			}
			pw.flush();
		}
	}

	/**
	 * Checks each object value is null(for all types) or NaN (for double type)
	 * @param obj 
	 * @return boolean 
	 */
	public static boolean isNullValue(Object obj){
		return (Objects.isNull(obj) || (obj instanceof Double && Double.isNaN((Double)obj)));
	}

	/**
	 * 
	 * @param urlString
	 */
	public static void checkIfValidDomain(String urlString) {
		String whiteListDomains =  Utility.getDIHelperProperty(Constants.WHITE_LIST_DOMAINS);
		if(whiteListDomains == null || (whiteListDomains=whiteListDomains.trim()).isEmpty()) {
			return;
		}
		
		List<String> domainList = Arrays.stream(whiteListDomains.split(",")).collect(Collectors.toList());
		URL url = null;
		try {
			url = new URL(urlString);
			final String host = url.getHost();
			final InternetDomainName domainName = InternetDomainName.from(host).topPrivateDomain();
			if(!domainList.contains(domainName.toString())) {
				throw new IllegalArgumentException("You are not allowed to make requests to the URL: " + urlString);
			}
		} catch (MalformedURLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Invalid URL: " + urlString + ". Detailed message: " + e.getMessage());
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public static ZonedDateTime getCurrentZonedDateTimeUTC() {
		ZonedDateTime zdt = ZonedDateTime.now();
		return convertZonedDateTimeToUTC(zdt);
	}
	
	/**
	 * 
	 * @param zdt
	 * @return
	 */
	public static ZonedDateTime convertZonedDateTimeToUTC(ZonedDateTime zdt) {
		ZonedDateTime gmt = zdt.withZoneSameInstant(ZoneId.of("UTC"));
		return gmt;
	}
	
	/**
	 * Get the current timestamp
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getCurrentSqlTimestampUTC() {
		return java.sql.Timestamp.valueOf(getCurrentZonedDateTimeUTC().toLocalDateTime());
	}
	
	/**
	 * Get the current timestamp
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(ZonedDateTime zdt) {
		ZonedDateTime gmt = zdt.withZoneSameInstant(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}
	
	/**
	 * Get the current timestamp
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(LocalDateTime ldt) {
		ZonedDateTime gmt = ldt.atZone(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}
	
	/**
	 * Get the current timestamp
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(SemossDate semossdate) {
		ZonedDateTime gmt = semossdate.getLocalDateTime().atZone(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}
	
	/**
	 * Get the current LocalDateTime
	 * @return currentTimestamp
	 */
	public static LocalDateTime getLocalDateTimeUTC(LocalDateTime ldt) {
		ZonedDateTime gmt = ldt.atZone(ZoneId.of("UTC"));
		return gmt.toLocalDateTime();
	}
	
	/**
	 * 
	 * @param user
	 * @return
	 */
	public static ZonedDateTime getCurrentZonedDateTimeForUser(User user) {
		ZoneId zoneId = ZoneId.of("UTC");
		if(user != null) {
			ZoneId userZoneId = user.getZoneId();
			if(userZoneId != null) {
				zoneId = userZoneId;
			}
		}
		
		return ZonedDateTime.now(zoneId);
	}
	

}
