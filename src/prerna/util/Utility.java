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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
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

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.pipeline.EngineProxyFactory;
import prerna.om.IStringExportProcessor;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskUtility;

/**
 * The Utility class contains a variety of miscellaneous functions implemented
 * extensively throughout SEMOSS. Some of these functionalities include getting
 * concept names, printing messages, loading engines, and writing Excel
 * workbooks.
 */
public final class Utility {

	private static final Logger classLogger = LogManager.getLogger(Utility.class);

	public static int id = 0;
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
		MySQLCodec mySQLCodec = new MySQLCodec(MySQLCodec.Mode.ANSI);
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

		MySQLCodec mySQLCodec = new MySQLCodec(MySQLCodec.Mode.ANSI);
		return ESAPI.encoder().encodeForSQL(mySQLCodec, (stringToNormalize));
	}

	/**
	 * Get the Base Folder
	 * 
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
	 * 
	 * @param prop
	 * @return
	 */
	public static String getDIHelperProperty(String prop) {
		if (DIHelper.getInstance() == null) {
			return null;
		}
		return DIHelper.getInstance().getProperty(prop);
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
		classLogger.debug("Param Hash is {}", paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key).get(0) + "";
			classLogger.debug("Replacing {}<<>>{}{}", key, value, query.indexOf("@" + key + "@"));
			if (!value.equalsIgnoreCase(Constants.EMPTY)) {
				query = query.replace("@" + key + "@", value);
			}
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
		classLogger.debug("Param Hash is {}", paramHash);

		Iterator keys = paramHash.keySet().iterator();
		while (keys.hasNext()) {
			String key = (String) keys.next();
			String value = paramHash.get(key);
//			classLogger.debug("Replacing " + key + "<<>>" + value + query.indexOf("@" + key + "@"));
			if (!value.equalsIgnoreCase(Constants.EMPTY)) {
				query = query.replace("@" + key + "@", value);
			}
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
	 * Makes sure that the file we are creating is in fact unique
	 * 
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
		while (f.exists()) {
			if (isH2) {
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
	public static String cleanString(String original, boolean replaceForwardSlash, boolean replaceForRDF,
			boolean property) {
		String retString = original;

		retString = retString.trim();
		retString = retString.replace("\t", " ");// replace tabs with spaces
		while (retString.contains("  ")) {
			retString = retString.replace("  ", " ");
		}
		retString = retString.replace("{", "(");
		retString = retString.replace("}", ")");
		retString = retString.replace("'", "");// remove apostrophe
		if (replaceForRDF) {
			retString = retString.replace("\"", "'");// replace double quotes with single quotes
		}
		retString = retString.replace(" ", "_");// replace spaces with underscores
		if (!property) {
			if (replaceForwardSlash) {
				retString = retString.replace("/", "-");// replace forward slashes with dashes
			}
			retString = retString.replace("\\", "-");// replace backslashes with dashes
		}

		retString = retString.replace("|", "-");// replace vertical lines with dashes
		retString = retString.replace("\n", " ");
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
			if (!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}

		try (FileOutputStream newExcelFile = new FileOutputStream(fileLocation)) {
			wb.write(newExcelFile);
			newExcelFile.flush();
		} catch (IOException ioe) {
			classLogger.error("Failed to write workbook to disk: {}", ioe.getMessage(), ioe);
		}
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
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query)) {
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
			classLogger.error("Failed to build vector query results: {}", e.getMessage(), e);
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
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query)) {
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
			classLogger.error("Failed to build vector array query results: {}", e.getMessage(), e);
		}

		return retArray;
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
		for (String formatString : date_formats) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(formatString);
				Date mydate = sdf.parse(input);
				SimpleDateFormat outdate = new SimpleDateFormat("yyyy-MM-dd");
				output_date = outdate.format(mydate);
				break;
			} catch (ParseException ignore) {
			}
		}

		return output_date;
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
		return (dataType.startsWith("IDENTITY") || dataType.startsWith("LONG") || dataType.startsWith("INT")
				|| dataType.startsWith("INTEGER") || dataType.startsWith("MEDIUMINT") || dataType.startsWith("INT4")
				|| dataType.startsWith("SIGNED") || dataType.startsWith("SERIAL")

				// TINYINT TYPE
				|| dataType.startsWith("TINYINT")

				// SMALLINT TYPE
				|| dataType.startsWith("SMALLINT") || dataType.startsWith("INT2") || dataType.startsWith("YEAR")

				// BIGINT TYPE
				|| dataType.startsWith("BIGINT") || dataType.startsWith("INT8")

				// PANDAS
				|| dataType.contains("DTYPE('INT64')"));
	}

	public static boolean isBoolean(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return (dataType.startsWith("BOOL") || dataType.startsWith("BIT"));
	}

	public static boolean isDoubleType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return (dataType.startsWith("NUMBER") || dataType.startsWith("MONEY") || dataType.startsWith("SMALLMONEY")
				|| dataType.startsWith("DECIMAL") || dataType.startsWith("DEC") || dataType.startsWith("NUMERIC")
				|| dataType.startsWith("DOUBLE") || dataType.startsWith("PRECISION") || dataType.startsWith("FLOAT")
				|| dataType.startsWith("FLOAT8")
				// REAL TYPE
				|| dataType.startsWith("REAL") || dataType.startsWith("FLOAT4")
				// PANDAS
				|| dataType.contains("DTYPE('FLOAT64')"));
	}

	public static boolean isStringType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return (dataType.equals("STRING")
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
				|| dataType.contains("DTYPE('O')"));
	}

	public static boolean isDateType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return dataType.equals("DATE");
	}

	public static boolean isTimeStamp(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return dataType.startsWith("TIMESTAMP") || dataType.startsWith("DATETIME");
	}

	public static boolean isFactorType(String dataType) {
		dataType = dataType.toUpperCase().trim();
		return dataType.startsWith("FACTOR") || dataType.startsWith("ORDER");
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
		// since we are using the fileName to be the table name,
		// let us try and remove this
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

	public static String getRandomString(int len) {
		String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";

		String retString = "a";
		SecureRandom secureRandom = new SecureRandom();
		for (int i = 0; i < len; i++) {
			double num = secureRandom.nextInt(alpha.length());
			retString = retString + alpha.charAt(Double.valueOf(num).intValue());
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

		if ((DIHelper.getInstance().getLocalProp("core") == null
				|| DIHelper.getInstance().getLocalProp("core").toString().equalsIgnoreCase("true"))) {
			if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
				project = (IProject) DIHelper.getInstance().getProjectProperty(projectId);
			} else {
				// Acquire the lock on the project, don't want several calls to try and load the
				// engine at the same time
				classLogger.info("Applying lock for project {}", Utility.cleanLogString(projectId));
				ReentrantLock lock = null;
				try {
					lock = ProjectSyncUtility.getProjectLock(projectId);
					lock.lock();
					classLogger.info("Project {} is locked", Utility.cleanLogString(projectId));

					// Need to do a double check here,
					// so if a different thread was waiting for the project to load,
					// it doesn't go through this process again
					if (DIHelper.getInstance().getProjectProperty(projectId) != null) {
						return EngineProxyFactory
								.createGuardedProject((IProject) DIHelper.getInstance().getProjectProperty(projectId));
					}

					// If in a clustered environment, then pull the project first
					if (pullIfNeeded && ClusterUtil.IS_CLUSTER) {
						ClusterUtil.pullProject(projectId);
					}

					// Now that the project has been pulled, grab the smss file
					String smssFile = (String) DIHelper.getInstance()
							.getProjectProperty(projectId + "_" + Constants.STORE);

					// Start up the project using the details in the smss
					if (smssFile != null) {
						// actual load engine process
						project = InternalUtility.loadProject(smssFile, Utility.loadProperties(smssFile));
						// a legacy insights db was recreated on load; replace the 1.x copy in storage
						if (project != null && ProjectHelper.consumeRecreatedLegacyInsights(projectId)) {
							try {
								ClusterUtil.pushInsightDB(projectId);
							} catch (Exception e) {
								classLogger.warn("Unable to push the recreated insights database for project {}",
										Utility.cleanLogString(projectId), e);
							}
						}
					} else {
						classLogger.debug("There is no SMSS File for the project {}...", projectId);
					}
				} finally {
					if (lock != null) {
						lock.unlock();
						classLogger.info("Project {} is unlocked", Utility.cleanLogString(projectId));
					}
				}
			}
		} else {
			String projectSock = projectId + "__SOCKET";

			if (DIHelper.getInstance().getProjectProperty(projectSock) != null) {
				project = (IProject) DIHelper.getInstance().getProjectProperty(projectSock);
			}
		}

		// pipeline
		return EngineProxyFactory.createGuardedProject(project);
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
		if (IEngine.CATALOG_TYPE.DATABASE == type) {
			return getDatabase(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.STORAGE == type) {
			return getStorage(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.MODEL == type) {
			return getModel(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.VECTOR == type) {
			return getVectorDatabase(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.FUNCTION == type) {
			return getFunctionEngine(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.GUARDRAIL == type) {
			return getGuardrailEngine(engineId, pullIfNeeded);
		} else if (IEngine.CATALOG_TYPE.VENV == type) {
			return getVenvEngine(engineId, pullIfNeeded);
		}

		throw new IllegalArgumentException("Unknown engine type with value " + type);
	}

	/**
	 * 
	 * @param engineId - engine to get
	 * @return Use this method to get the engine when the engine hasn't been loaded
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedDatabaseEngine((IDatabaseEngine) engine);
		return (IDatabaseEngine) engine;
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedStorageEngine((IStorageEngine) engine);
		return (IStorageEngine) engine;
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedModelEngine((IModelEngine) engine);
		return (IModelEngine) engine;
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedVectorEngine((IVectorDatabaseEngine) engine);
		return (IVectorDatabaseEngine) engine;
	}

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IReactorFunctionEngine getReactorEngine(String engineId) {
		return getReactorEngine(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IReactorFunctionEngine getReactorEngine(String engineId, boolean pullIfNeeded) {
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedFunctionEngine((IReactorFunctionEngine) engine);
		return (IReactorFunctionEngine) engine;
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedFunctionEngine((IFunctionEngine) engine);
		return (IFunctionEngine) engine;
	}

	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static IGuardrailReactorFunctionEngine getGuardrailEngine(String engineId) {
		return getGuardrailEngine(engineId, true);
	}

	/**
	 * 
	 * @param engineId
	 * @param pullIfNeeded
	 * @return
	 */
	public static IGuardrailReactorFunctionEngine getGuardrailEngine(String engineId, boolean pullIfNeeded) {
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedFunctionEngine((IFunctionEngine) engine);
		return (IGuardrailReactorFunctionEngine) engine;
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
		IEngine engine = InternalUtility.baseGetEngine(engineId, pullIfNeeded);
		// get the pipeline
		engine = EngineProxyFactory.createGuardedVenvEngine((IVenvEngine) engine);
		return (IVenvEngine) engine;
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
			classLogger.error("Failed to write result rows to output file: {}", ioe.getMessage(), ioe);
		}

		try (FileOutputStream fos = new FileOutputStream(f);
				OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
				BufferedWriter bufferedWriter = new BufferedWriter(osw)) {

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
						if (dataRow[i] == null) {
							builder.append("\"\"");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							builder.append("\"").append(thisStringVal).append("\"");
						}
					} else {
						// print out null
						if (dataRow[i] == null) {
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
				if (it instanceof IRawSelectWrapper) {
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
						if (dataRow[i] == null) {
							builder.append("\"\"");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							builder.append("\"").append(thisStringVal).append("\"");
						}
					} else {
						// print out null
						if (dataRow[i] == null) {
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
			classLogger.error("Failed to write result rows to output file: {}", ioe.getMessage(), ioe);
		}

		long end = System.currentTimeMillis();
		classLogger.info("Time to output file = {} ms. File written to:{}", (end - start),
				Utility.normalizePath(fileLocation));

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
			classLogger.error("Failed to write result rows to JSON file: {}", ioe.getMessage(), ioe);
		}

		try (FileOutputStream fos = new FileOutputStream(f);
				OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
				BufferedWriter bufferedWriter = new BufferedWriter(osw)) {
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
						if (dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if (dataRow[i] == null || (dataRow[i] instanceof Double && Double.isNaN((double) dataRow[i]))
								|| (dataRow[i] instanceof Double && Double.isInfinite((double) dataRow[i]))) {
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
						if (dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if (dataRow[i] == null || (dataRow[i] instanceof Double && Double.isNaN((double) dataRow[i]))
								|| (dataRow[i] instanceof Double && Double.isInfinite((double) dataRow[i]))) {
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
			classLogger.error("Failed to write result rows to JSON file: {}", ioe.getMessage(), ioe);
		}

		long end = System.currentTimeMillis();
		classLogger.info("Time to output file = {} ms. File written to:{}", (end - start),
				Utility.normalizePath(fileLocation));

		return f;
	}

	public static JSONArray writeResultToJsonObject(Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap,
			IStringExportProcessor... exportProcessors) {
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
						if (dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if (dataRow[i] == null) {
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
						if (dataRow[i] == null) {
							json.put(headers[i], "");
						} else {
							String thisStringVal = null;
							if (dataRow[i] instanceof Object[]) {
								thisStringVal = Arrays.toString((Object[]) dataRow[i]);
							} else {
								thisStringVal = dataRow[i] + "";
							}
							if (exportProcessors != null) {
								for (IStringExportProcessor process : exportProcessors) {
									thisStringVal = process.processString(thisStringVal);
								}
							}
							json.put(headers[i], thisStringVal);
						}
					} else {
						// print out null
						if (dataRow[i] == null) {
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
		classLogger.info("Time to output file = {} ms.", (end - start));
		return jsonArray;
	}

	public static String encodeURIComponent(String s) {
		if (s == null) {
			return null;
		}
		s = URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20").replace("!", "\\%21").replace("'", "\\%27")
				.replace("(", "\\%28").replace(")", "\\%29").replace("~", "\\%7E");
		return s;
	}

	public static String decodeURIComponent(String s) {
		return decodeURIComponent(s, true);
	}

	public static String decodeURIComponent(String s, boolean performCheck) {
		if (s == null) {
			return null;
		}
		if (performCheck) {
			final Pattern PERCENT_ENCODED = Pattern.compile("%[0-9A-Fa-f]{2}");
			if (!PERCENT_ENCODED.matcher(s).find()) {
				return s; // can't be percent-encoded, return as-is
			}
		}

		String newS = s.replace("%20", "+").replace("%21", "!").replace("%27", "'").replace("%28", "(")
				.replace("%29", ")").replace("%7E", "~");
		s = URLDecoder.decode(newS, StandardCharsets.UTF_8);
		return s;
	}

	// ensure no CRLF injection into logs for forging records
	public static String cleanLogString(String message) {
		if (message == null) {
			return message;
		}
		message = message.replace('\n', '_').replace('\r', '_').replace('\t', '_');

		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.LOG_ENCODING) + "")) {
			message = ESAPI.encoder().encodeForHTML(message);
		}

		return message;
	}

	// ensure no CRLF injection into responses for malicious attacks
	public static String cleanHttpResponse(String message) {
		if (message == null) {
			return message;
		}
		message = message.replace('\n', '_').replace("%0d", "_").replace('\r', '_').replace("%0a", "_")
				.replace('\t', '_').replace("%09", "_");

		message = Encode.forHtml(message);
		return message;
	}

	/**
	 * 
	 * @param stringToNormalize
	 * @return
	 */
	public static String normalizePath(String stringToNormalize) {
		if (stringToNormalize == null) {
			return stringToNormalize;
		}
		// replacing \\ with /
		stringToNormalize = stringToNormalize.replace("\\", "/");
		// ensuring no double //
		while (stringToNormalize.contains("//")) {
			stringToNormalize = stringToNormalize.replace("//", "/");
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

	// true when the value (e.g. an id from the DB or a remote API) is one folder/file name: not blank, not . or .., no separator or NUL
	// checked again after NFKC, since normalizePath applies it and full-width forms can turn into / \ or .
	public static boolean isSafePathSegment(String value) {
		return value != null && isSingleName(value) && isSingleName(Normalizer.normalize(value, Form.NFKC));
	}

	private static boolean isSingleName(String value) {
		return !value.isBlank() && !".".equals(value) && !"..".equals(value) && value.indexOf('/') < 0
				&& value.indexOf('\\') < 0 && value.indexOf('\0') < 0;
	}

	// returns the value, or logs and throws IllegalArgumentException when it is not a single safe name
	public static String requireSafePathSegment(String value, String what) {
		if (!isSafePathSegment(value)) {
			classLogger.warn("Refused {} that is not a single safe folder name: {}", what, cleanLogString(value));
			throw new IllegalArgumentException("Not a valid " + what);
		}
		return value;
	}

	// one name (e.g. an id from the DB or a remote API) under a folder; logs and throws IllegalArgumentException
	// unless it is a single safe name whose normalized path is a direct child of the folder
	public static String childPath(String folder, String name, String what) {
		requireSafePathSegment(name, what);
		Path base = Path.of(folder).toAbsolutePath().normalize();
		Path child = base.resolve(name).normalize();
		if (!child.startsWith(base) || !base.equals(child.getParent())) {
			classLogger.warn("Refused {} that leaves its folder: {}", what, cleanLogString(name));
			throw new IllegalArgumentException("Not a valid " + what);
		}
		// same string form callers had before (folder + separator + name), from the checked name
		return folder + File.separator + child.getFileName();
	}

	// a relative path (sub folders allowed) under a folder; logs and throws IllegalArgumentException when it is
	// blank, absolute, changes under NFKC or would leave the folder; returns the normalized path
	public static String pathWithin(String folder, String relative, String what) {
		if (relative == null || relative.isBlank() || relative.indexOf('\0') >= 0
				|| !relative.equals(Normalizer.normalize(relative, Form.NFKC))) {
			classLogger.warn("Refused {} that is not a plain relative path: {}", what, cleanLogString(relative));
			throw new IllegalArgumentException("Not a valid " + what);
		}
		Path base = Path.of(folder).toAbsolutePath().normalize();
		Path rel = Path.of(relative.replace('\\', '/'));
		Path target = base.resolve(rel).normalize();
		if (rel.isAbsolute() || !target.startsWith(base) || target.equals(base)) {
			classLogger.warn("Refused {} that leaves its folder: {}", what, cleanLogString(relative));
			throw new IllegalArgumentException("Not a valid " + what);
		}
		return target.toString();
	}

	/**
	 * Resolves a file inside a folder and returns the real location of the file.
	 * The file path must be relative to the folder. Both the folder and the file
	 * are resolved to their real locations, so neither a {@code ..} segment nor a
	 * symbolic link inside the folder can reach a file outside it.
	 *
	 * @param folderPath   the folder the file must be in
	 * @param relativePath the file, relative to the folder
	 * @return the real location of the file
	 * @throws IOException              when the folder or the file cannot be found
	 * @throws IllegalArgumentException when either path is blank, or the file path
	 *                                  is absolute, leads outside the folder or
	 *                                  names something other than a regular file
	 */
	public static Path resolveContainedFile(String folderPath, String relativePath) throws IOException {
		if (folderPath == null || folderPath.isBlank()) {
			throw new IllegalArgumentException("The folder must not be blank");
		}
		if (relativePath == null || relativePath.isBlank()) {
			throw new IllegalArgumentException("The file path must not be blank");
		}

		Path reference = Path.of(relativePath);
		if (reference.isAbsolute()) {
			throw new IllegalArgumentException("The file path " + relativePath + " must be relative to its folder");
		}

		Path root = Path.of(folderPath).toAbsolutePath().normalize().toRealPath();
		Path candidate = root.resolve(reference).normalize();
		if (!candidate.startsWith(root)) {
			throw new IllegalArgumentException("The file path " + relativePath + " is outside its folder");
		}

		Path file = candidate.toRealPath();
		if (!file.startsWith(root)) {
			throw new IllegalArgumentException("The file path " + relativePath + " links outside its folder");
		}
		if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
			throw new IllegalArgumentException("The file path " + relativePath + " is not a file");
		}
		return file;
	}

	/**
	 *
	 * @param filePaths
	 * @return
	 */
	public static List<String> normalizeFilePaths(List<String> filePaths) {
		if (filePaths == null) {
			throw new NullPointerException("File paths cannot be null");
		}

		List<String> normalizedPaths = new ArrayList<>(filePaths.size());
		for (String rawFilePath : filePaths) {
			if (rawFilePath == null) {
				throw new NullPointerException("File path cannot be null");
			}

			String normalizedPath = normalizePath(rawFilePath.trim());
			if (normalizedPath == null || normalizedPath.isEmpty()) {
				throw new IllegalArgumentException("Must provide a valid filePath");
			}
			normalizedPaths.add(normalizedPath);
		}
		return normalizedPaths;
	}

	/**
	 * Loads the properties from a specified properties file.
	 * 
	 * @param filePath String of the name of the properties file to be loaded.
	 * @return Properties The properties imported from the prop file.
	 */
	public static Properties loadProperties(String filePath) {
		Properties retProp = new Properties();
		if (filePath != null) {
			try (FileInputStream fis = new FileInputStream(Utility.normalizePath(filePath))) {
				retProp.load(fis);
			} catch (IOException ioe) {
				classLogger.error("Failed to load properties file: {}", ioe.getMessage(), ioe);
			}
		}
		for (String name : retProp.stringPropertyNames()) {
			String value = retProp.getProperty(name);
			if (value != null) {
				retProp.setProperty(name, value.trim());
			}
		}
		return retProp;
	}

	/**
	 * Loads the properties from a specified properties file.
	 * 
	 * @param file File object to load
	 * @return Properties The properties imported from the prop file.
	 */
	public static Properties loadProperties(File file) {
		Properties retProp = new Properties();
		if (file != null) {
			try (FileInputStream fis = new FileInputStream(file)) {
				retProp.load(fis);
			} catch (IOException ioe) {
				classLogger.error("Failed to load properties file: {}", ioe.getMessage(), ioe);
			}
		}
		for (String name : retProp.stringPropertyNames()) {
			String value = retProp.getProperty(name);
			if (value != null) {
				retProp.setProperty(name, value.trim());
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
		try (StringReader is = new StringReader(propertiesAsString)) {
			try {
				retProp.load(is);
			} catch (IOException e) {
				classLogger.error("Failed to load properties from string input: {}", e.getMessage(), e);
			}
		}
		return retProp;
	}

	/**
	 * Determine if for this instance only the admin can add a project
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_PROJECT_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a project
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_PROJECT_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set project access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_PROJECT_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set insight access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_INSIGHT_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a project public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_PROJECT_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set the project
	 * discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyProjectSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_PROJECT_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	public static boolean getApplicationAdminOnlyWorkspaceAdd() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_WORKSPACE_ADD));
	}

	public static boolean getApplicationAdminOnlyWorkspaceDelete() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_WORKSPACE_DELETE));
	}

	public static boolean getApplicationAdminOnlyWorkspaceAddAccess() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_WORKSPACE_ADD_ACCESS));
	}

	public static boolean getApplicationAdminOnlyWorkspaceSetPublic() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_WORKSPACE_SET_PUBLIC));
	}

	public static boolean getApplicationAdminOnlyWorkspaceSetDiscoverable() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_WORKSPACE_SET_DISCOVERABLE));
	}

	public static boolean getApplicationAdminOnlySkillAdd() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_SKILL_ADD));
	}

	public static boolean getApplicationAdminOnlySkillDelete() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_SKILL_DELETE));
	}

	public static boolean getApplicationAdminOnlySkillAddAccess() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_SKILL_ADD_ACCESS));
	}

	public static boolean getApplicationAdminOnlySkillSetPublic() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_SKILL_SET_PUBLIC));
	}

	public static boolean getApplicationAdminOnlySkillSetDiscoverable() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.ADMIN_ONLY_SKILL_SET_DISCOVERABLE));
	}

	/**
	 * Determine if for this instance only the admin can share insight
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightShare() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_INSIGHT_SHARE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set an insight public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyInsightSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_INSIGHT_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a database
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_DB_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a database
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_DB_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set database access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_DB_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a database public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_DB_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a database discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyDbSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_DB_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a model
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyModelAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_MODEL_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a model
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyModelDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_MODEL_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set model access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyModelAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_MODEL_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a model public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyModelSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_MODEL_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a model discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyModelSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_MODEL_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a storage
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyStorageAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_STORAGE_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a storage
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyStorageDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_STORAGE_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set storage access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyStorageAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_STORAGE_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a storage public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyStorageSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_STORAGE_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a storage discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyStorageSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_STORAGE_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a vector
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyVectorAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VECTOR_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a vector
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyVectorDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VECTOR_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set vector access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyVectorAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VECTOR_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a vector public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyVectorSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VECTOR_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a vector discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyVectorSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VECTOR_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a function
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyFunctionAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_FUNCTION_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a function
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyFunctionDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_FUNCTION_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set function access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyFunctionAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_FUNCTION_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a function public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyFunctionSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_FUNCTION_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a function discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyFunctionSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_FUNCTION_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a guardrail
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyGuardrailAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_GUARDRAIL_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a guardrail
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyGuardrailDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_GUARDRAIL_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set guardrail access
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyGuardrailAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_GUARDRAIL_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a guardrail public
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyGuardrailSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_GUARDRAIL_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a guardrail
	 * discoverable
	 * 
	 * @return
	 */
	public static boolean getApplicationAdminOnlyGuardrailSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_GUARDRAIL_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add a virtual environment
	 * 
	 * @return true if only an admin can add a virtual environment
	 */
	public static boolean getApplicationAdminOnlyVenvAdd() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VENV_ADD);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can delete a virtual
	 * environment
	 * 
	 * @return true if only an admin can delete a virtual environment
	 */
	public static boolean getApplicationAdminOnlyVenvDelete() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VENV_DELETE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can add/set virtual environment
	 * access
	 * 
	 * @return true if only an admin can add/set virtual environment access
	 */
	public static boolean getApplicationAdminOnlyVenvAddAccess() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VENV_ADD_ACCESS);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a virtual environment
	 * public
	 * 
	 * @return true if only an admin can set a virtual environment public
	 */
	public static boolean getApplicationAdminOnlyVenvSetPublic() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VENV_SET_PUBLIC);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Determine if for this instance only the admin can set a virtual environment
	 * discoverable
	 * 
	 * @return true if only an admin can set a virtual environment discoverable
	 */
	public static boolean getApplicationAdminOnlyVenvSetDiscoverable() {
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VENV_SET_DISCOVERABLE);
		if (boolString == null) {
			// default false
			return false;
		}

		return Boolean.parseBoolean(boolString);
	}

	/**
	 * Get a comma separated list of widget ids to filter on
	 * 
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationPipelineLandingFilter() {
		String filterList = Utility.getDIHelperProperty(Constants.PIPELINE_LANDING_FILTER);
		if (filterList == null || (filterList = filterList.trim()).isEmpty()) {
			// default null
			return null;
		}

		return filterList.split(",");
	}

	/**
	 * Get a comma separated list of widget ids to filter on
	 * 
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationPipelineSourceFilter() {
		String filterList = Utility.getDIHelperProperty(Constants.PIPELINE_SOURCE_FILTER);
		if (filterList == null || (filterList = filterList.trim()).isEmpty()) {
			// default null
			return null;
		}

		return filterList.split(",");
	}

	/**
	 * Get a comma separated list of widget ids to filter on
	 * 
	 * @return
	 */
	@Deprecated
	public static String[] getApplicationWidgetTabShareExportList() {
		String filterList = Utility.getDIHelperProperty(Constants.WIDGET_TAB_SHARE_EXPORT_LIST);
		if (filterList == null || (filterList = filterList.trim()).isEmpty()) {
			// default null
			return null;
		}

		return filterList.split(",");
	}

	/**
	 * Determine if on the application we should cache insights or not
	 * 
	 * @return
	 */
	public static boolean getApplicationCacheInsight() {
		String cacheSetting = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHT_CACHEABLE);
		if (cacheSetting == null) {
			// default cache is true
			return true;
		}

		return Boolean.parseBoolean(cacheSetting);
	}

	/**
	 * Determine amount of time to cache insights by default
	 * 
	 * @return
	 */
	public static int getApplicationCacheInsightMinutes() {
		String cacheSetting = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHT_CACHE_MINUTES);
		if (cacheSetting == null) {
			// default is no limit
			return -1;
		}

		return Integer.parseInt(cacheSetting);
	}

	/**
	 * Determine cron schedule for the cache existence
	 * 
	 * @return
	 */
	public static String getApplicationCacheCron() {
		String cacheSetting = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHT_CACHE_CRON);
		if (cacheSetting == null) {
			// default is false
			return null;
		}

		cacheSetting = cacheSetting.trim();

		if (!CronExpression.isValidExpression(cacheSetting)) {
			classLogger.error("Application DEFAULT_INSIGHT_CACHE_CRON value '{}' is not a valid cron expression",
					cacheSetting);
			return null;
		}

		return cacheSetting;
	}

	/**
	 * Determine if the cache should be encrypted by default or not
	 * 
	 * @return
	 */
	public static boolean getApplicationCacheEncrypt() {
		String cacheSetting = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHT_CACHE_ENCRYPT);
		if (cacheSetting == null) {
			// default is false
			return false;
		}

		return Boolean.parseBoolean(cacheSetting);
	}

	/**
	 * Determine if HashiCorp Vault is enabled for secrets
	 * 
	 * @return
	 */
	public static boolean isSecretsStoreEnabled() {
		String hashiCorpEnabled = Utility.getDIHelperProperty(Constants.SECRET_STORE_ENABLED);
		if (hashiCorpEnabled == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(hashiCorpEnabled);
	}

	/**
	 * Determine if virus scanner enabled
	 * 
	 * @return
	 */
	public static boolean isVirusScanningEnabled() {
		String virusScanning = Utility.getDIHelperProperty(Constants.VIRUS_SCANNING_ENABLED);
		if (virusScanning == null) {
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
	 * 
	 * @return
	 */
	public static boolean isUserTrackingEnabled() {
		String userTracking = Utility.getDIHelperProperty(Constants.USER_TRACKING_ENABLED);
		if (userTracking == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(userTracking);
	}

	/**
	 * Determine if model inference logs db is enabled
	 * 
	 * @return
	 */
	public static boolean isModelInferenceLogsEnabled() {
		String modelInferenceLogs = Utility.getDIHelperProperty(Constants.MODEL_INFERENCE_LOGS_ENABLED);
		if (modelInferenceLogs == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(modelInferenceLogs);
	}

	/**
	 * Determine if promptdb logs db is enabled
	 * 
	 * @return
	 */
	public static boolean isPromptDatabaseEnabled() {
		String promptDB = Utility.getDIHelperProperty(Constants.PROMPT_DB_ENABLED);
		if (promptDB == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(promptDB);
	}

	/**
	 * Determine if audit logs db is enabled
	 * 
	 * @return
	 */
	public static boolean isAuditLogsDatabaseEnabled() {
		String auditLogsDb = Utility.getDIHelperProperty(Constants.AUDIT_LOGS_DATABASE_ENABLED);
		if (auditLogsDb == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(auditLogsDb);
	}

	/**
	 * Determine if user tracking enabled
	 * 
	 * @return
	 */
	public static boolean schedulerForceDisable() {
		String schedulerForceDisable = Utility.getDIHelperProperty(Constants.SCHEDULER_FORCE_DISABLE);
		if (schedulerForceDisable == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(schedulerForceDisable);
	}

	public static boolean isUserTrackingDisabled() {
		return !isUserTrackingEnabled();
	}

	public static String getUserTrackingMethod() {
		return Utility.getDIHelperProperty(Constants.USER_TRACKING_METHOD);
	}

	/**
	 * 
	 * @return
	 */
	public static String getDefaultTerminalMode() {
		String terminalMode = Utility.getDIHelperProperty(Constants.TERMINAL_MODE);
		if (terminalMode == null || (terminalMode = terminalMode.trim()).isEmpty()) {
			// default configuration is false
			return "cmd";
		}

		List<String> valid = new ArrayList<>();
		valid.add("cmd");
		valid.add("powershell");

		if (!valid.contains(terminalMode) && !valid.contains(terminalMode.toLowerCase())) {
			classLogger.warn("Invalid terminal mode = {}. Switching to cmd for windows and bash for mac/linux.",
					terminalMode);
		}

		return terminalMode;
	}

	/**
	 * Determine if We need to show Welcome Dialog on Application Load
	 * 
	 * @return
	 */
	public static boolean getWelcomeBannerOption() {
		String welcomeDialog = Utility.getDIHelperProperty(Constants.SHOW_WELCOME_BANNER);
		if (welcomeDialog == null) {
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
		String timeZone = Utility.getDIHelperProperty(Constants.DEFAULT_TIME_ZONE);
		if (timeZone == null || (timeZone = timeZone.trim()).isEmpty()) {
			// default to ET
			return "America/New_York";
		}

		return timeZone.trim();
	}

	/**
	 * Get the application zone id
	 * 
	 * @return
	 */
	public static String getApplicationZoneId() {
		String timeZone = Utility.getDIHelperProperty(Constants.DEFAULT_TIME_ZONE);
		if (timeZone == null || (timeZone = timeZone.trim()).isEmpty()) {
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
		String boolString = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_CREATE_API_USER);
		if (boolString == null || (boolString = boolString.trim()).isEmpty()) {
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
		String sameSiteString = Utility.getDIHelperProperty(Constants.SAMESITE_COOKIE);
		if (sameSiteString == null || (sameSiteString = sameSiteString.trim()).isEmpty()) {
			return "none";
		}

		if (!sameSiteString.equalsIgnoreCase("strict") && !sameSiteString.equalsIgnoreCase("lax")
				&& !sameSiteString.equalsIgnoreCase("none")) {
			classLogger.warn("Invalid samesite cookie option = '{}'. Must be 'strict', 'lax', or 'none'",
					sameSiteString);
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
			URL redirectUrl = new URI(redirectUrlStr).toURL();
			String protocol = redirectUrl.getProtocol();
			int port = redirectUrl.getPort();
			String host = redirectUrl.getHost();
			if (port > 0) {
				return protocol + "://" + host + ":" + port;
			} else {
				return protocol + "://" + host;
			}
		} catch (URISyntaxException | MalformedURLException e) {
			classLogger.error("Failed to resolve application base URL: {}", e.getMessage(), e);
		}
		return null;
	}

	public static String getApplicationContextPath() {
		String contextPath = (String) DIHelper.getInstance().getLocalProp(Constants.CONTEXT_PATH_KEY);
		if (contextPath == null || (contextPath = contextPath.trim()).isEmpty()) {
			return null;
		}

		if (contextPath.startsWith("/")) {
			contextPath = contextPath.substring(1);
		}
		if (contextPath.endsWith("/")) {
			contextPath = contextPath.substring(0, contextPath.length() - 1);
		}
		return contextPath;
	}

	public static String getApplicationOptionalRoutePath() {
		String route = Utility.getDIHelperProperty(Constants.MONOLITH_ROUTE);
		if (route == null) {
			Map<String, String> envMap = System.getenv();
			if (envMap.containsKey(Constants.MONOLITH_ROUTE)) {
				route = envMap.get(Constants.MONOLITH_ROUTE);
			}
		}
		if (route == null || (route = route.trim()).isEmpty()) {
			return null;
		}

		if (route.startsWith("/")) {
			route = route.substring(1);
		}
		if (route.endsWith("/")) {
			route = route.substring(0, route.length() - 1);
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
		if (optionalRoute != null && !(optionalRoute = optionalRoute.trim()).isEmpty()) {
			url = url + "/" + optionalRoute;
		}
		url = url + "/" + contextPath;
		return url;
	}

	/**
	 * 
	 * @return
	 */
	public static String getApplicationRouteAndContextPath() {
		String optionalRoute = getApplicationOptionalRoutePath();
		String contextPath = getApplicationContextPath();
		String routeAndContext = "/" + contextPath;
		if (optionalRoute != null && !(optionalRoute = optionalRoute.trim()).isEmpty()) {
			routeAndContext = "/" + optionalRoute + routeAndContext;
		}
		return routeAndContext;
	}

	/**
	 * Get the name of the FE webapp. Default to SemossWeb
	 * 
	 * @return
	 */
	public static String getFEWebAppName() {
		String feWebAppName = "SemossWeb";
		if (Utility.getDIHelperProperty(Constants.FE_WEB_APP_NAME) != null) {
			String candidate = Utility.getDIHelperProperty(Constants.FE_WEB_APP_NAME);
			if (candidate != null && !(candidate = candidate.trim()).isEmpty()) {
				feWebAppName = candidate;
			}
		}
		return feWebAppName;
	}

	/**
	 * Default value is public_home
	 * 
	 * @return
	 */
	public static String getPublicHomeFolder() {
		String publicHomeFolder = "public_home";
		if (Utility.getDIHelperProperty(Settings.PUBLIC_HOME) != null) {
			publicHomeFolder = Utility.getDIHelperProperty(Settings.PUBLIC_HOME);
		}
		// assume public home is clean for lower paths
		if (publicHomeFolder.startsWith("/")) {
			publicHomeFolder = publicHomeFolder.substring(1);
		}
		if (publicHomeFolder.endsWith("/")) {
			publicHomeFolder = publicHomeFolder.substring(0, publicHomeFolder.length() - 1);
		}
		return publicHomeFolder;
	}

	/**
	 * Checks each object value is null(for all types) or NaN (for double type)
	 * 
	 * @param obj
	 * @return boolean
	 */
	public static boolean isNullValue(Object obj) {
		return (Objects.isNull(obj) || (obj instanceof Double && Double.isNaN((Double) obj)));
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
	 * 
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getCurrentSqlTimestampUTC() {
		return java.sql.Timestamp.valueOf(getCurrentZonedDateTimeUTC().toLocalDateTime());
	}

	/**
	 * Get the current timestamp
	 * 
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(ZonedDateTime zdt) {
		ZonedDateTime gmt = zdt.withZoneSameInstant(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}

	/**
	 * Get the current timestamp
	 * 
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(LocalDateTime ldt) {
		ZonedDateTime gmt = ldt.atZone(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}

	/**
	 * Get the current timestamp
	 * 
	 * @return currentTimestamp
	 */
	public static java.sql.Timestamp getSqlTimestampUTC(SemossDate semossdate) {
		ZonedDateTime gmt = semossdate.getLocalDateTime().atZone(ZoneId.of("UTC"));
		return java.sql.Timestamp.valueOf(gmt.toLocalDateTime());
	}

	/**
	 * A timestamp that holds UTC time, as {@link #getCurrentSqlTimestampUTC()}
	 * gives, as ISO text, such as 2026-10-29T18:30:00Z.
	 *
	 * @param timestamp the UTC timestamp
	 * @return the ISO text, ending in Z
	 */
	public static String getSqlTimestampToString(Timestamp timestamp) {
		return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toString();
	}

	/**
	 * Get the current LocalDateTime
	 * 
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
		if (user != null) {
			ZoneId userZoneId = user.getZoneId();
			if (userZoneId != null) {
				zoneId = userZoneId;
			}
		}

		return ZonedDateTime.now(zoneId);
	}

	// helper to validate names across webapp
	public static Boolean validateName(String name) {
		String regex = "^[a-zA-Z][a-zA-Z0-9 _-]*$";
		return name.matches(regex);
	}

	/**
	 * 
	 * @param size
	 * @return
	 */
	public static String getReadableFileSize(long size) {
		if (size <= 0) {
			return "0 B";
		}
		final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
	}

	/**
	 * Determine if admins only are able to view left hand menu
	 * 
	 * @return
	 */
	public static boolean getAdminOnlyViewMenuBarFlag() {
		String coreFlag = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_VIEW_MENU_BAR);
		if (coreFlag == null) {
			// default option is true
			return true;
		}

		return Boolean.parseBoolean(coreFlag);
	}

	/**
	 * Determine if admins only are able to create non approved engines
	 * 
	 * @return
	 */
	public static boolean getAdminOnlyNonApprovedFlag() {
		String nonApprovedFlag = Utility.getDIHelperProperty(Constants.ADMIN_ONLY_NON_APPROVED_PROD_ITEM);
		if (nonApprovedFlag == null) {
			// default option is true
			return true;
		}

		return Boolean.parseBoolean(nonApprovedFlag);
	}

	/**
	 * Returns true only if the folder contains at least one direct non-directory
	 * file. Use this when you specifically need to confirm flat files exist (e.g.,
	 * validating that a folder has been populated with data files, not just
	 * sub-folders).
	 */
	public static boolean folderHasAnyFiles(String folderPath) {
		File folder = new File(folderPath);
		if (!folder.exists() || !folder.isDirectory()) {
			return false;
		}
		// Check for at least one non-directory file
		File[] files = folder.listFiles(f -> f.isFile());
		return files != null && files.length > 0;
	}

	/**
	 * Returns true if the folder exists and contains any entries - files,
	 * sub-directories, hidden files, dot files, etc. Use this when you just need to
	 * know the folder is non-empty regardless of whether its contents are files or
	 * directories (e.g., before syncing a room folder to cloud storage where the
	 * room may store data in sub-directories).
	 */
	public static boolean folderIsNotEmpty(String folderPath) {
		File folder = new File(folderPath);
		if (!folder.exists() || !folder.isDirectory()) {
			return false;
		}
		File[] entries = folder.listFiles();
		return entries != null && entries.length > 0;
	}

	/**
	 * Determine if notification db is enabled
	 * 
	 * @return
	 */
	public static boolean isNotificationDatabaseEnabled() {
		String notificationDb = Utility.getDIHelperProperty(Constants.NOTIFICATION_DATABASE_ENABLED);
		if (notificationDb == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(notificationDb);
	}

	/**
	 * Determine if collaboration db is enabled
	 * 
	 * @return
	 */
	public static boolean isCollaborationDatabaseEnabled() {
		String collaborationDb = Utility.getDIHelperProperty(Constants.COLLABORATION_DATABASE_ENABLED);
		if (collaborationDb == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(collaborationDb);
	}

	/**
	 * Determine if the web search platform MCP is enabled. It needs a SearXNG
	 * service that not every instance runs, so it stays off unless RDF_Map turns it
	 * on.
	 *
	 * @return
	 */
	public static boolean isWebSearchEnabled() {
		String webSearch = Utility.getDIHelperProperty(Constants.WEB_SEARCH_ENABLED);
		if (webSearch == null) {
			// default configuration is false
			return false;
		}

		return Boolean.parseBoolean(webSearch.trim());
	}

	public static DocumentBuilderFactory getDocumentBuilderFactory() {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try {
			factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
			factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
			factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
			factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			factory.setXIncludeAware(false); // Do not allow XInclude
			factory.setExpandEntityReferences(false);
		} catch (ParserConfigurationException e) {
			throw new IllegalArgumentException("Unable to get DocumentBuilderFactory instance.");
		}
		return factory;
	}

}
