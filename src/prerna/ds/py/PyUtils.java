package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

public class PyUtils {

	private static final Logger classLogger = LogManager.getLogger(PyUtils.class.getName());

	public static final String PY_COMMAND_SEPARATOR = ";";

	private static Boolean pyEnabled = null;
	private static PyUtils instance;
	public Map<User, String> userTupleMap = new Hashtable<>();
	public Map<User, Process> userProcessMap = new Hashtable<>();

	private PyUtils() {

	}

	public static PyUtils getInstance() throws IllegalArgumentException {
		if (instance == null) {
			setPyEnabled();
			if (pyEnabled) {
				instance = new PyUtils();
			}
		}

		return instance;
	}

	/**
	 * Method to set internally for this class if python is enabled
	 */
	private static void setPyEnabled() {
		if (pyEnabled == null) {
			pyEnabled = false;
			String usePythonStr = Utility.getDIHelperProperty(Constants.USE_PYTHON);
			if (usePythonStr != null) {
				pyEnabled = Boolean.parseBoolean(usePythonStr);
			}
		}
	}

	/**
	 * Getter if python is enabled
	 * 
	 * @return
	 */
	public static boolean pyEnabled() {
		if (pyEnabled == null) {
			pyEnabled = false;
			String usePythonStr = Utility.getDIHelperProperty(Constants.USE_PYTHON);
			if (usePythonStr != null) {
				pyEnabled = Boolean.parseBoolean(usePythonStr);
			}
		}
		return pyEnabled;
	}

	/**
	 * This is basically a utility method that attempts to generate the python code
	 * (string) for a java object. It currently only does base types. Potentially
	 * move it in the future but just keeping it here for now
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
	 * 
	 * @param str
	 * @return
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
	 * This is good for python dictionaries but also for making sure we can easily
	 * construct the logs into model inference python list, since everything is
	 * python at this point.
	 * 
	 * @param map
	 * @return
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

	private static String appendPythonExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "/python.exe";
		} else {
			return interpreterDir + "/bin/python3";
		}
	}

	public static String appendVenvPythonExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "Scripts/python.exe";
		} else {
			return interpreterDir + "/bin/python3";
		}
	}

	public static String appendVenvPipExecutable(String interpreterDir) {
		if (SystemUtils.IS_OS_WINDOWS) {
			return interpreterDir + "/Scripts/pip.exe";
		} else {
			return interpreterDir + "/bin/pip3";
		}
	}

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
		throw new IOException("Unable to find site packages directory for OS");
	}

	public static List<String> getPythonHomeSitePackages() throws IOException {
		String mainPySitePackages = Utility.getDIHelperProperty(Settings.PYTHONHOME_SITE_PACKAGES);
		if (mainPySitePackages == null || (mainPySitePackages = mainPySitePackages.trim()).isEmpty()) {
			String pythonExecutablePath = appendPythonExecutable(getPythonHomeDir());
			ProcessBuilder processBuilder = new ProcessBuilder(pythonExecutablePath, "-c",
					"\"import site; import json; print(json.dumps(site.getsitepackages()))\"");

			try {
				Process process = processBuilder.start();
				java.io.BufferedReader reader = new java.io.BufferedReader(
						new java.io.InputStreamReader(process.getInputStream()));

				// Read the input stream using a BufferedReader and collect lines into a list
				String sitePackagePathsString = reader.lines().collect(Collectors.joining(System.lineSeparator()));
				// Create a TypeToken for List<String>
				TypeToken<List<String>> token = new TypeToken<List<String>>() {
				};
				// Use Gson to deserialize the JSON string into a List<String>
				List<String> sitePackagePaths = new Gson().fromJson(sitePackagePathsString, token.getType());
				process.waitFor();

				return sitePackagePaths;
			} catch (IOException | InterruptedException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IOException("Unable to find site packages using python home executable");
			}
		} else {
			return Arrays.asList(mainPySitePackages);
		}
	}

	public static String[] applyUlimit(String[] commands) {
		// need to make sure we are not windows cause ulimit will not work
		if (!SystemUtils.IS_OS_WINDOWS
				&& !(Strings.isNullOrEmpty(Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT)))) {
			String ulimit = Utility.getDIHelperProperty(Constants.ULIMIT_R_MEM_LIMIT);
			StringBuilder sb = new StringBuilder();
			for (String str : commands) {
				sb.append(str).append(" ");
			}
			sb.substring(0, sb.length() - 1);
			commands = new String[] { "/bin/bash", "-c", "\"ulimit -v " + ulimit + " && " + sb.toString() + "\"" };
		}
		return commands;
	}
}
