package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.ICache;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class PyTranslator {

	private static final Logger classLogger = LogManager.getLogger(PyTranslator.class);

	public static final String METHOD_DELIMITER = "$$##";
	public static String curEncoding = null;

	protected Logger logger = null;
	protected Insight insight = null;

	private SocketClient sc = null;
	private String method = null;
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	static Map<String, SemossDataType> pyS = new Hashtable<String, SemossDataType>();
	static {
		pyS.put("object", SemossDataType.STRING);
		pyS.put("category", SemossDataType.STRING);
		pyS.put("int64", SemossDataType.INT);
		pyS.put("float64", SemossDataType.DOUBLE);
		pyS.put("datetime64", SemossDataType.DATE);
		pyS.put("datetime64[ns]", SemossDataType.TIMESTAMP);
	}

	public PyTranslator() {
		this.logger = LogManager.getLogger(PyTranslator.class);
	}

	public SemossDataType convertDataType(String pDataType) {
		return pyS.get(pDataType);
	}

	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	// sets the insight
	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(String script) {
		return (List<Object>) runScript(script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(String script) {
		List<String> val = (List<String>) runScript(script);
		return val;
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public String[] getStringArray(String script) {
		List<String> val = getStringList(script);
		String[] retString = new String[val.size()];
		val.toArray(retString);
		return retString;
	}

	/**
	 * Get boolean from py script
	 * 
	 * @param script
	 * @return
	 */
	public boolean getBoolean(String script) {
		Boolean x = (Boolean) runScript(script);
		return x.booleanValue();
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(String script) {
		Double x = getLong(script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Double getLong(String script) {
		// TODO remove if else once we no longer use JEP
		// JEP -> Long
		// PyServer -> Double
		Object x = runScript(script);
		if (x instanceof Long) {
			Long y = (Long) x;
			return y.doubleValue();
		} else if (x instanceof Double) {
			return (Double) x;
		} else if (x instanceof String) {
			return Double.valueOf((String) x);
		} else {
			return null;
		}
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(String script) {
		Double x = (Double) runScript(script);
		return x.doubleValue();
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(String script) {
		return (String) runScript(script);
	}

	public void runEmptyPy(String... script) {
		// get the insight folder
		// create a teamp to write the script file
		String pyTemp = null;
		if (this.insight != null) {
			pyTemp = this.insight.getInsightFolder().replace('\\', '/') + "/py/Temp/";
		} else {
			pyTemp = (Utility.getBaseFolder() + "/Py/Temp/").replace('\\', '/');
		}

		File pyTempF = new File(Utility.normalizePath(pyTemp));
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
		}

		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight != null) {
				if (this.insight.getUser() != null) {
					this.insight.getUser().getUserSymlinkHelper().symlinkFolder(pyTemp);
				}
			}
		}

		String scriptFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp + scriptFileName + ".py";
		File scriptFile = new File(Utility.normalizePath(scriptPath));

		try {
			String finalScript = convertArrayToString(script);
			FileUtils.writeStringToFile(scriptFile, finalScript, Charset.forName("UTF-8"));

			// the wrapper needs to be run now
			// executePyDirect("runwrapper(" + scriptPath + "," + outPath + "," + outPath +
			// ")");
			// executePyDirect("smssutil.run_empty_wrapper(\"" + scriptPath + "\",
			// globals())");
			// changing this to runscript
			runScript("smssutil.run_empty_wrapper(\"" + scriptPath + "\", globals())");
		} catch (IOException e1) {
			// System.out.println("Error in writing Py script for execution!");
			classLogger.error(Constants.STACKTRACE, e1);
		} finally {
			// Cleanup
			scriptFile.delete();
			// TODO - when fake insights are added, change back to delete folder
			// ICache.deleteFolder(pyTempF);
		}
	}

	public String runPyAndReturnOutput(String... inscript) {
		// Clean the script
		String script = convertArrayToString(inscript);
		script = script.trim();

		// find if the script is simple
		boolean multi = (inscript.length > 1 || script.contains("\n")) || script.contains("=")
				|| (script.contains(".") && script.endsWith("()")) && !script.equals("dir()");

		// Get temp folder and file locations
		// also define a ROOT variable
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;

		String pyTemp = null;
		if (this.insight != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT = '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = " ROOT";

			// context project takes precedence
			if (this.insight.getContextProjectId() != null) {
				appRootPath = AssetUtility.getProjectAssetFolder(this.insight.getContextProjectName(), this.insight.getContextProjectId());
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			} else if (this.insight.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
				userRootPath = userRootPath.replace('\\', '/');
				userRootAssignment = "USER_ROOT = '" + userRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", USER_ROOT";
			} catch (Exception ignore) {
				// ignore
			}

			pyTemp = insightRootPath + "/Py/Temp/";
		} else {
			pyTemp = (Utility.getBaseFolder() + "/Py/Temp/").replace('\\', '/');
		}

		if (!removePathVariables.isEmpty()) {
			removePathVariables = "del " + removePathVariables;
		}

		File pyTempF = new File(Utility.normalizePath(pyTemp));
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
			pyTempF.setExecutable(true);
			pyTempF.setReadable(true);
			pyTempF.setReadable(true);
		}

		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight.getUser() != null) {
				this.insight.getUser().getUserSymlinkHelper().symlinkFolder(pyTemp);
			}
		}

		String pyFileName = Utility.getRandomString(12);
		String prePyName = Utility.getRandomString(5);
		String scriptPath = pyTemp + pyFileName + ".py";
		String preScriptPath = pyTemp + prePyName + ".py";
		File scriptFile = new File(Utility.normalizePath(scriptPath));
		File preScriptFile = new File(Utility.normalizePath(preScriptPath));
		String outputPath = pyTemp + pyFileName + ".txt";
		File outputFile = new File(Utility.normalizePath(outputPath));

		multi = true;

		if (script.startsWith("@")) {
			multi = false;
		}

		// attempt to put it into environment
		String preScript = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment;

		if (multi) {
			// Try writing the script to a file
			try {
				FileUtils.writeStringToFile(preScriptFile, preScript, Charset.forName("UTF-8"));
				// execute all the commands for setting variables etc.
				executeEmptyPyDirect("exec(open('" + preScriptPath + "').read())", null);
				FileUtils.writeStringToFile(scriptFile, script, Charset.forName("UTF-8"));

				// check packages
				// checkPackages(script);

				// Try running the script, which saves the output to a file
				// TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R
				// (later)
				RuntimeException error = null;
				try {
					executeEmptyPyDirect("smssutil.runwrapper(\"" + scriptPath + "\", \"" + outputPath + "\", \""
							+ outputPath + "\", globals())", null);
					// executeEmptyPyDirect2("smssutil.runwrapper(\"" + scriptPath + "\", \"" +
					// outputPath + "\", \"" + outputPath + "\", globals())", outputPath);
				} catch (RuntimeException e) {
					classLogger.error(Constants.STACKTRACE, e);
					error = e; // Save the error so we can report it
				}

				// Finally, read the output and return, or throw the appropriate error
				try {
					String output = FileUtils.readFileToString(outputFile, Charset.forName("UTF-8")).trim();
					// Error cases

					// clean up the output
					if (userRootPath != null && output.contains(userRootPath)) {
						output = output.replace(userRootPath, "$USER_IF");
					}
					if (appRootPath != null && output.contains(appRootPath)) {
						output = output.replace(appRootPath, "$APP_IF");
					}
					if (insightRootPath != null && output.contains(insightRootPath)) {
						output = output.replace(insightRootPath, "$IF");
					}

					if (error != null) {
						throw error;
					}

					// Successful case
					return output;
				} catch (IOException e) {
					// If we have the detailed error, then throw it
					if (error != null) {
						throw error;
					}

					// Otherwise throw a generic one
					throw new IllegalArgumentException("Failed to run Py script.");
				} finally {
					// Cleanup
					outputFile.delete();
					if (!removePathVariables.isEmpty()) {
						try {
							this.runEmptyPy(removePathVariables);
							// this.executeEmptyR("gc();"); // Garbage collection
						} catch (Exception e) {
							logger.warn("Unable to cleanup Py.", e);
						}
					}
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error in writing Py script for execution.");
			} finally {

				// Cleanup
				scriptFile.delete();
				preScriptFile.delete();
			}
		} else {
			String finalScript = convertArrayToString(inscript);
			finalScript = finalScript.replace("@", "");
			Object scriptResponse = runScript(finalScript);
			if (scriptResponse instanceof SemossPixelException) {
				throw (SemossPixelException) scriptResponse;
			} else {
				return scriptResponse + "";
			}
		}
	}

	public synchronized String runSingle(String inscript, Insight in) {
		// Clean the script
		String script = convertArrayToString(inscript);
		script = script.trim();

		// define variables
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;

		String pyTemp = null;
		if (in != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT = '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = " ROOT";

			// context project takes precedence
			if (in.getContextProjectId() != null) {
				appRootPath = AssetUtility.getProjectAssetFolder(in.getContextProjectName(), in.getContextProjectId());
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			} else if (in.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
				userRootPath = userRootPath.replace('\\', '/');
				userRootAssignment = "USER_ROOT = '" + userRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", USER_ROOT";
			} catch (Exception ignore) {
				// ignore
			}

			pyTemp = insightRootPath + "/Py/Temp/";
		} else {
			pyTemp = (Utility.getBaseFolder() + "/Py/Temp/").replace('\\', '/');
		}

		if (!removePathVariables.isEmpty()) {
			removePathVariables = "del " + removePathVariables;
		}

		File pyTempF = new File(pyTemp);
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
			pyTempF.setExecutable(true);
			pyTempF.setReadable(true);
			pyTempF.setReadable(true);
		}

		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight.getUser() != null) {
				this.insight.getUser().getUserSymlinkHelper().symlinkFolder(insightRootPath);
			}
		}

		String pyFileName = Utility.getRandomString(12);
		String prePyName = Utility.getRandomString(5);
		String scriptPath = pyTemp + pyFileName + ".py";
		String preScriptPath = pyTemp + prePyName + ".py";
		File scriptFile = new File(scriptPath);
		File preScriptFile = new File(preScriptPath);

		// attempt to put it into environment
		String preScript = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment;
		String output = null;
		try {
			FileUtils.writeStringToFile(preScriptFile, preScript, Charset.forName("UTF-8"));
			executeEmptyPyDirect("exec(open('" + preScriptPath + "').read())", in);
			FileUtils.writeStringToFile(scriptFile, script, Charset.forName("UTF-8"));

			// Try running the script, which saves the output to a file
			// TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R
			// (later)
			RuntimeException error = null;
			try {
				// Start the error sender thread
				Object pythonReturnObject = runSmssWrapperEval(script, insight);

				if (pythonReturnObject instanceof String) {
					output = (String) pythonReturnObject;
				} else {
					try {
						output = new GsonBuilder().disableHtmlEscaping().create().toJson(pythonReturnObject);
					} catch (Exception e) {
						output = pythonReturnObject + "";
					}
				}
			} catch (RuntimeException e) {
				classLogger.error(Constants.STACKTRACE, e);
				error = e; // Save the error so we can report it
			}

			// Finally, read the output and return, or throw the appropriate error
			try {
				// Error cases

				// clean up the output
				if (userRootPath != null && output.contains(userRootPath)) {
					output = output.replace(userRootPath, "$USER_IF");
				}
				if (appRootPath != null && output.contains(appRootPath)) {
					output = output.replace(appRootPath, "$APP_IF");
				}
				if (insightRootPath != null && output.contains(insightRootPath)) {
					output = output.replace(insightRootPath, "$IF");
				}

				// Successful case
				return output;
			} catch (Exception e) {
				// If we have the detailed error, then throw it
				if (error != null) {
					throw error;
				}

				// Otherwise throw a generic one
				throw new IllegalArgumentException("Failed to run Py script.");
			} finally {
				// Cleanup
				try {
					if (!removePathVariables.isEmpty()) {
						this.runScript(removePathVariables);
					}
				} catch (Exception e) {
					logger.warn("Unable to remove path variables", e);
				}
			}
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error in writing Py script for execution.");
		} finally {
			// cleanup
			preScriptFile.delete();
			scriptFile.delete();
		}
	}

	/**
	 * 
	 * @param script
	 * @param in
	 * @return
	 */
	public String runScript(String script, Insight in) {
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;

		if (in != null) {
			insightRootPath = in.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT = '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = ", ROOT";

			// context project takes precedence
			if (in.getContextProjectId() != null) {
				appRootPath = AssetUtility.getProjectAssetFolder(in.getContextProjectName(), in.getContextProjectId());
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			} else if (in.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
				userRootPath = userRootPath.replace('\\', '/');
				userRootAssignment = "USER_ROOT = '" + userRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", USER_ROOT";
			} catch (Exception ignore) {
				// ignore
			}
		}

		String assignmentString = insightRootAssignment + appRootAssignment + userRootAssignment;
		executeEmptyPyDirect(assignmentString, in);
		String output = runScript(script) + "";

		// clean up the output
		if (userRootPath != null && output.contains(userRootPath)) {
			output = output.replace(userRootPath, "$USER_IF");
		}
		if (appRootPath != null && output.contains(appRootPath)) {
			output = output.replace(appRootPath, "$APP_IF");
		}
		if (insightRootPath != null && output.contains(insightRootPath)) {
			output = output.replace(insightRootPath, "$IF");
		}

		// Successful case
		return output;
	}

	protected String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder("");
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++) {
			if (script[lineIndex] != null) {
				retString.append(script[lineIndex]).append("\n");
			}
		}
		return retString.toString();
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	// this becomes an issue on windows where it only consumes specific encoding
	public String getCurEncoding() {
		if (curEncoding == null) {
			curEncoding = runPyAndReturnOutput("print(sys.stdout.encoding)");
		}
		return curEncoding;
	}

	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "list(" + frameName + ".columns)";
		List<String> colNames = (List<String>) runScript(script);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	}

	/**
	 * 
	 * @param sc
	 */
	public void setSocketClient(SocketClient sc) {
		this.sc = sc;
	}
	
	/**
	 * 
	 * @return
	 */
	public SocketClient getSocketClient() {
		return this.sc;
	}

	public Object runScript(String script)  {
		if(method != null) {
			script = method + METHOD_DELIMITER + script;
			method = null;
		}
		
		//System.out.println(".");
//		Object response = nc.executeCommand(script);
		//Object [] outputObj = (Object [])response;
		
		//System.out.println("Command was " + outputObj[0] + "<>" + script + "<>" + outputObj[1]);
		//System.err.println("Got the response back !!!!! WOO HOO " + response);
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = constructPayload(methodName, script);
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		if(sc.isConnected()) {
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			} else {
				return ps.payload[0];
			}
		} else {
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	// use this if we want to get the output from an operation
	// typically useful for model type operations
	public Object runSmssWrapperEval(String script, Insight insight) {
		if(method != null) {
			script = method + METHOD_DELIMITER + script;
			method = null;
		}

		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = constructPayload(methodName, script);
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		
		// get error messages
		if(insight != null) {
			ps.insightId = insight.getInsightId();
		}

		if(sc.isConnected()) {
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			} else {
				return ps.payload[0];
			}
		} else {
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * 
	 * @param script
	 * @param in
	 */
	protected void executeEmptyPyDirect(String script, Insight in) {
		runSmssWrapperEval(script, in);
	}

	/**
	 * 
	 * @param methodName
	 * @param objects
	 * @return
	 */
	private PayloadStruct constructPayload(String methodName, Object...objects ) {
		// go through the objects and if they are set to null then make them as string null
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.R;
		ps.methodName = methodName;
		ps.payload = objects;
		
		return ps;
	}	

}
