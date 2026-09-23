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
package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

public final class PyUtils {

	private static final Logger classLogger = LogManager.getLogger(PyUtils.class.getName());

	public static final String PY_COMMAND_SEPARATOR = ";";

	private static Boolean pyEnabled = null;

	/**
	 * Indicates whether Python integrations are enabled.
	 *
	 * @return {@code true} when Python usage is enabled; {@code false} otherwise
	 */
	public static boolean pyEnabled() {
		if (pyEnabled == null) {
			boolean proxy = false;
			String usePythonStr = Utility.getDIHelperProperty(Constants.USE_PYTHON);
			if (usePythonStr != null) {
				proxy = Boolean.parseBoolean(usePythonStr);
			}
			pyEnabled = proxy;
		}
		return pyEnabled;
	}

	/**
	 * Converts a Java object into a Python-safe literal string representation.
	 *
	 * @param obj object to convert
	 * @return Python literal string for the provided object
	 */
	@SuppressWarnings("unchecked")
	public static String determineStringType(Object obj) {
		if (obj == null) {
			return "None";
		} else if (obj instanceof Integer || obj instanceof Double || obj instanceof Long || obj instanceof Float) {
			return String.valueOf(obj);
		} else if (obj instanceof Map) {
			return constructPyDictFromMap((Map<String, Object>) obj);
		} else if (obj instanceof ArrayList || obj instanceof Object[] || obj instanceof List) {
			StringBuilder theList = new StringBuilder("[");
			List<Object> list;

			if (obj instanceof ArrayList<?>) {
				list = (ArrayList<Object>) obj;
			} else if (obj instanceof Object[]) {
				list = Arrays.asList((Object[]) obj);
			} else {
				list = (List<Object>) obj;
			}

			boolean isFirstElement = true;
			for (Object subObj : list) {
				if (!isFirstElement) {
					theList.append(", "); // Add space after comma for readability
				} else {
					isFirstElement = false;
				}
				theList.append(determineStringType(subObj));
			}
			theList.append("]");
			return theList.toString();
		} else if (obj instanceof Boolean) {
			// Python uses True/False (capitalized)
			return ((Boolean) obj) ? "True" : "False";
		} else if (obj instanceof Set<?>) {
			StringBuilder theSet = new StringBuilder("{");
			Set<?> set = (Set<?>) obj;
			boolean isFirstElement = true;
			for (Object subObj : set) {
				if (!isFirstElement) {
					theSet.append(", "); // Add space after comma
				} else {
					isFirstElement = false;
				}
				theSet.append(determineStringType(subObj));
			}
			theSet.append("}");
			return theSet.toString();
		} else if (obj instanceof File) {
			// Use forward slashes and proper raw string formatting
			String path = ((File) obj).getAbsolutePath().replace("\\", "/");
			return "r\"" + path + "\""; // Use double quotes to avoid conflicts with single quotes in path
		} else if (obj instanceof Insight) {
			// Assuming Insight has a proper string representation
			String insightId = ((Insight) obj).getInsightId();
			return "\"" + escapeString(insightId) + "\""; // Properly escape the string
		} else {
			// Handle general string case with proper escaping
			String str = String.valueOf(obj);
			return "\"" + escapeString(str) + "\""; // Use double quotes and proper escaping
		}
	}

	/**
	 * Escapes Java string content so it can be safely embedded into Python strings.
	 *
	 * @param str raw string content
	 * @return escaped string content, or an empty string when {@code str} is
	 *         {@code null}
	 */
	private static String escapeString(String str) {
		if (str == null) {
			return "";
		}

		return str.replace("\\", "\\\\") // Escape backslashes first
				.replace("\"", "\\\"") // Escape double quotes
				.replace("\n", "\\n") // Escape newlines
				.replace("\r", "\\r") // Escape carriage returns
				.replace("\t", "\\t"); // Escape tabs
	}

	/**
	 * Quote-and-escape a Java string into a single-quoted Python string literal.
	 * Returns "None" for null.
	 */
	public static String pyQuote(String value) {
		if (value == null) {
			return "None";
		}
		StringBuilder sb = new StringBuilder(value.length() + 4);
		sb.append('\'');
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			switch (c) {
			case '\\':
				sb.append("\\\\");
				break;
			case '\'':
				sb.append("\\'");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			default:
				if (c < 0x20) {
					sb.append(String.format("\\x%02x", (int) c));
				} else {
					sb.append(c);
				}
			}
		}
		sb.append('\'');
		return sb.toString();
	}

	/**
	 * This is good for python dictionaries but also for making sure we can easily
	 * construct the logs into model inference python list, since everything is
	 * python at this point.
	 *
	 * @param map map to render as a Python dictionary
	 * @return Python dictionary literal string
	 */
	private static String constructPyDictFromMap(Map<String, Object> map) {
		StringBuilder dict = new StringBuilder("{");
		boolean isFirst = true;

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			if (!isFirst) {
				dict.append(", ");
			} else {
				isFirst = false;
			}

			// Key should be a string in Python dict
			dict.append("\"").append(escapeString(entry.getKey())).append("\"");
			dict.append(": ");
			dict.append(determineStringType(entry.getValue()));
		}

		dict.append("}");
		return dict.toString();
	}

	/**
	 * Validates whether a string can be used as a Python variable identifier.
	 *
	 * @param name candidate variable name
	 * @return {@code true} if the name is a valid non-keyword Python identifier
	 */
	public static boolean isValidPythonVariableName(String name) {
		// Check if the string is null or empty
		if (name == null || name.isEmpty()) {
			return false;
		}

		// Check if it's a Python keyword
		if (isPythonKeyword(name)) {
			return false;
		}

		// First character must be a letter (a-z, A-Z) or underscore
		char firstChar = name.charAt(0);
		if (!Character.isLetter(firstChar) && firstChar != '_') {
			return false;
		}

		// Remaining characters must be letters, digits, or underscores
		for (int i = 1; i < name.length(); i++) {
			char c = name.charAt(i);
			if (!Character.isLetterOrDigit(c) && c != '_') {
				return false;
			}
		}

		return true;
	}

	/**
	 * Checks whether a string matches a reserved Python keyword.
	 *
	 * @param name value to check
	 * @return {@code true} if the value is a Python keyword
	 */
	private static boolean isPythonKeyword(String name) {
		String[] keywords = { "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
				"continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
				"in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with",
				"yield" };

		for (String keyword : keywords) {
			if (keyword.equals(name)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks whether {@code pypi.org} is reachable from the current runtime.
	 *
	 * @return {@code true} when PyPI can be reached; {@code false} otherwise
	 */
	public static boolean isPyPIReachable() {
		try {
			// Try to reach pypi.org with a timeout of 1000 milliseconds
			java.net.InetAddress.getByName("pypi.org").isReachable(1000);
			return true;
		} catch (java.io.IOException e) {
			// An exception occurred, indicating no internet connection to PyPI
			return false;
		}
	}

	/**
	 * Resolves the configured Python home directory from environment variables and
	 * platform settings.
	 *
	 * @return configured Python home directory
	 * @throws NullPointerException when no Python home is configured
	 */
	public static String getPythonHomeDir() {
		String py = System.getenv(Settings.PYTHONHOME);
		if (py == null) {
			py = Utility.getDIHelperProperty(Settings.PYTHONHOME);
		}
		if (py == null) {
			System.getenv(Settings.PY_HOME);
		}
		if (py == null) {
			py = Utility.getDIHelperProperty(Settings.PY_HOME);
		}
		if (py == null) {
			throw new NullPointerException("Must define python home");
		}
		return py;
	}

	/**
	 * Appends the expected Python executable to the provided interpreter directory.
	 *
	 * @param interpreterDir base interpreter directory
	 * @return full path to the platform-specific Python executable
	 */
	private static String appendPythonExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "/python.exe";
		} else {
			return interpreterDir + "/bin/python3";
		}
	}

	/**
	 * Appends the virtual environment Python executable to the provided directory.
	 *
	 * @param interpreterDir base virtual environment directory
	 * @return full path to the virtual environment Python executable
	 */
	public static String appendVenvPythonExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "Scripts/python.exe";
		} else {
			return interpreterDir + "/bin/python3";
		}
	}

	/**
	 * Appends the virtual environment pip executable to the provided directory.
	 *
	 * @param interpreterDir base virtual environment directory
	 * @return full path to the virtual environment pip executable
	 */
	public static String appendVenvPipExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "/Scripts/pip.exe";
		} else {
			return interpreterDir + "/bin/pip3";
		}
	}

	/**
	 * Resolves the site-packages directory for a Python interpreter home.
	 *
	 * @param interpreterDir interpreter home directory
	 * @return full path to the site-packages directory
	 * @throws IOException when the directory cannot be determined for the platform
	 */
	public static String appendSitePackagesPath(String interpreterDir) throws IOException {
		if (SystemUtils.IS_OS_WINDOWS) {
			if (new File(interpreterDir + "/Lib/site-packages").isDirectory()) {
				return interpreterDir + "/Lib/site-packages";
			}
		} else {
			String libDirPath = interpreterDir + "/lib";
			File libDir = new File(libDirPath);
			if (libDir.exists() && libDir.isDirectory()) {
				File[] libSubDirs = libDir.listFiles(File::isDirectory);
				if (libSubDirs != null && libSubDirs.length > 0) {
					// Filter subdirectories based on naming convention for Python versions
					File pythonVersionDir = Arrays.stream(libSubDirs)
							.filter(subDir -> subDir.getName().startsWith("python")).findFirst().orElse(null);

					if (pythonVersionDir != null) {
						String pythonVersion = pythonVersionDir.getName();
						return libDirPath + "/" + pythonVersion + "/site-packages";
					}
				}
			}

		}
		throw new IOException("Unable to find site-packages directory under '" + interpreterDir + "' for OS '"
				+ System.getProperty("os.name") + "'");
	}

	/**
	 * Returns site-packages paths for the configured Python home.
	 *
	 * @return list of site-packages directories
	 * @throws IOException when site-packages cannot be determined
	 */
	public static List<String> getPythonHomeSitePackages() throws IOException {
		String mainPySitePackages = Utility.getDIHelperProperty(Settings.PYTHONHOME_SITE_PACKAGES);
		if (mainPySitePackages == null || (mainPySitePackages = mainPySitePackages.trim()).isEmpty()) {
			String pythonExecutablePath = appendPythonExecutable(getPythonHomeDir());
			ProcessBuilder processBuilder = new ProcessBuilder(pythonExecutablePath, "-c",
					"\"import site; import json; print(json.dumps(site.getsitepackages()))\"");
			String commandDescription = String.join(" ", processBuilder.command());
			Process process;
			try {
				process = processBuilder.start();
			} catch (IOException e) {
				String message = "Failed to start Python site-packages command: " + commandDescription;
				classLogger.error(message, e);
				throw new IOException(message, e);
			}

			try (java.io.BufferedReader reader = process.inputReader()) {
				// Read the input stream using a BufferedReader and collect lines into a list
				String sitePackagePathsString = reader.lines().collect(Collectors.joining(System.lineSeparator()));
				// Create a TypeToken for List<String>
				TypeToken<List<String>> token = new TypeToken<List<String>>() {
				};
				// Use Gson to deserialize the JSON string into a List<String>
				List<String> sitePackagePaths = new Gson().fromJson(sitePackagePathsString, token.getType());
				process.waitFor();

				return sitePackagePaths;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				String message = "Interrupted while waiting for Python site-packages command to finish: "
						+ commandDescription;
				classLogger.error(message, e);
				throw new IOException(message, e);
			} catch (IOException e) {
				String message = "Failed to read output from Python site-packages command: " + commandDescription;
				classLogger.error(message, e);
				throw new IOException(message, e);
			}
		} else {
			return Arrays.asList(mainPySitePackages);
		}
	}

	/**
	 * Private constructor for utility class.
	 */
	private PyUtils() {

	}

}
