package prerna.ds.py;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.project.impl.Project;
import prerna.tcp.client.ErrorSenderThread;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyTranslator {

	// this will start to become the main interfacing class for everything related
	// to python
	// this is the equivalent of doing RTranslator on the R end

	// this is the default all of the pandas stuff would use

	// sets the insight
	Insight insight = null;

	Logger logger = null;

	PyExecutorThread pt = null;
	public static String curEncoding = null;
	public static String NO_OUTPUT = "<e>";
	public static String NEED_OUTPUT = "<o>";

	Map<Object, Object> responseCache = new HashMap<Object, Object>();
	String internalLock = "something that the translator will wait on and will be informed by event handler";
	// TODO need to replace this duplicate code from PandasFrame
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
		// startDisruptor();
		// System.out.println("Py Translator created");
		this.logger = LogManager.getLogger(Project.class);
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

	public void setPy(PyExecutorThread pt) {
		this.pt = pt;
		// this.outBuffer = pt.getBuffer();
	}

	public PyExecutorThread getPy() {
		return this.pt;
		// this.outBuffer = pt.getBuffer();
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
		ArrayList<String> val = (ArrayList<String>) runScript(script);
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
		} else if (x instanceof String){
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

	protected synchronized Hashtable executePyDirect(String... script) {
		if (this.pt == null)
			this.pt = insight.getPy();

		Object monitor = pt.getMonitor();
		synchronized (monitor) {
			pt.command = script;
			monitor.notify();
			try {
				monitor.wait();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			logger.info("Completed processing");
		}
		return pt.response;
	}

	protected synchronized void executeEmptyPyDirect(String script, Insight in) {
		if (this.pt == null)
			this.pt = insight.getPy();

		Object monitor = pt.getMonitor();
		synchronized (monitor) {
			pt.command = new String[] { script };
			monitor.notify();
			try {
				monitor.wait();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			logger.info("Completed processing");

		}

	}

	public synchronized void runEmptyPy(String... script) {
		// get the insight folder
		// create a teamp to write the script file
		String rootPath = null;
		String pyTemp = null;
		String addRootVariable = "";
		if (this.insight != null) {
			rootPath = this.insight.getInsightFolder().replace('\\', '/');
			pyTemp = rootPath + "/py/Temp/";
			addRootVariable = "ROOT <- '" + rootPath.replace("'", "\\'") + "';";
			String removeRootVar = "ROOT";
		} else {
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}

		File pyTempF = new File(Utility.normalizePath(pyTemp));
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
		}

		String scriptFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp + scriptFileName + ".py";
		File scriptFile = new File(Utility.normalizePath(scriptPath));

		try {
			String finalScript = convertArrayToString(script);
			FileUtils.writeStringToFile(scriptFile, finalScript);

			// the wrapper needs to be run now
			// executePyDirect("runwrapper(" + scriptPath + "," + outPath + "," + outPath +
			// ")");
			// executePyDirect("smssutil.run_empty_wrapper(\"" + scriptPath + "\",
			// globals())");
			// changing this to runscript
			runScript("smssutil.run_empty_wrapper(\"" + scriptPath + "\", globals())");

		} catch (IOException e1) {
			// System.out.println("Error in writing Py script for execution!");
			e1.printStackTrace();
		}
	}

	public synchronized String runPyAndReturnOutput(String... inscript) {
		return runPyAndReturnOutput(null, inscript);
	}

	public synchronized String runPyAndReturnOutput(Map<String, StringBuffer> appMap, String... inscript) {
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

			if (this.insight.isSavedInsight()) {
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
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}

		removePathVariables = "del " + removePathVariables;
		// get the custom var String
		String varFolderAssignment = "";
		if (appMap != null && appMap.containsKey("PY_VAR_STRING"))
			varFolderAssignment = appMap.get("PY_VAR_STRING").toString();

		File pyTempF = new File(Utility.normalizePath(pyTemp));
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
			pyTempF.setExecutable(true);
			pyTempF.setReadable(true);
			pyTempF.setReadable(true);
		}

		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight.getUser() != null) {
				this.insight.getUser().getUserMountHelper().mountFolder(pyTemp, pyTemp, false);
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
		String preScript = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment + "\n"
				+ varFolderAssignment;

		if (multi) {
			// Try writing the script to a file
			try {
				FileUtils.writeStringToFile(preScriptFile, preScript);
				// execute all the commands for setting variables etc.
				executeEmptyPyDirect("exec(open('" + preScriptPath + "').read())", null);

				FileUtils.writeStringToFile(scriptFile, script);

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
					e.printStackTrace();
					error = e; // Save the error so we can report it
				}

				// Finally, read the output and return, or throw the appropriate error
				try {
					String output = FileUtils.readFileToString(outputFile).trim();
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
					if (varFolderAssignment != null && varFolderAssignment.length() > 0) {
						output = cleanCustomVar(output, appMap);
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
					try {
						this.runEmptyPy(removePathVariables);
						// this.executeEmptyR("gc();"); // Garbage collection
					} catch (Exception e) {
						logger.warn("Unable to cleanup Py.", e);
					}
				}
			} catch (IOException e) {
				throw new IllegalArgumentException("Error in writing Py script for execution.", e);
			} finally {

				// Cleanup
				scriptFile.delete();
				preScriptFile.delete();
			}
		} else {
			String finalScript = convertArrayToString(inscript);
			finalScript = finalScript.replace("@", "");
			Hashtable response = executePyDirect(finalScript);
			return response.get(finalScript) + "";
		}
	}

	public synchronized String runSingle(Map<String, StringBuffer> appMap, String inscript, Insight in) 
	{
		
		
		// Clean the script
		String script = convertArrayToString(inscript);
		script = script.trim();

		// find if the script is simple

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

			if (this.insight.isSavedInsight()) {
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
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}
		removePathVariables = "del " + removePathVariables;

		// get the custom var String
		String varFolderAssignment = "";
		if (appMap != null && appMap.containsKey("PY_VAR_STRING"))
			varFolderAssignment = appMap.get("PY_VAR_STRING").toString();

		File pyTempF = new File(pyTemp);
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
			pyTempF.setExecutable(true);
			pyTempF.setReadable(true);
			pyTempF.setReadable(true);
		}

		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight.getUser() != null) {
				this.insight.getUser().getUserMountHelper().mountFolder(pyTemp, pyTemp, false);
			}
		}

		String pyFileName = Utility.getRandomString(12);
		String prePyName = Utility.getRandomString(5);
		String scriptPath = pyTemp + pyFileName + ".py";
		String preScriptPath = pyTemp + prePyName + ".py";
		File scriptFile = new File(scriptPath);
		File preScriptFile = new File(preScriptPath);
		String outputPath = pyTemp + pyFileName + ".txt";
		File outputFile = new File(outputPath);

		// attempt to put it into environment
		String preScript = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment + "\n"
				+ varFolderAssignment;
		// execute all the commands for setting variables etc.
		// Try writing the script to a file
		ErrorSenderThread est = new ErrorSenderThread();
		if(in != null)
		{
			est.setInsight(in);
			est.start();
			est.setFile(outputPath);
		}				

		String output = null;
		try {
			FileUtils.writeStringToFile(preScriptFile, preScript);
			executeEmptyPyDirect("exec(open('" + preScriptPath + "').read())", null);
			FileUtils.writeStringToFile(scriptFile, script);

			// check packages
			// checkPackages(script);

			// Try running the script, which saves the output to a file
			// TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R
			// (later)
			RuntimeException error = null;
			try {
				// Start the error sender thread
				if(this instanceof TCPPyTranslator && ((TCPPyTranslator)this).nc instanceof NativePySocketClient)
					output = "" + runScript(script, insight);
				else
				{
					runScript("smssutil.runwrappereval(\"" + scriptPath + "\", \"" + outputPath + "\", \""
									+ outputPath + "\", globals())");

					if(in != null)
					{
						est.stopSession();
					}
					output = FileUtils.readFileToString(outputFile).trim();
				}					
			} catch (RuntimeException e) {
				e.printStackTrace();
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
				if (varFolderAssignment != null && varFolderAssignment.length() > 0) {
					output = cleanCustomVar(output, appMap);
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
				if(outputFile.exists())
				{
					try {
					outputFile.delete();
					this.runScript(removePathVariables);
					// this.executeEmptyR("gc();"); // Garbage collection
					} catch (Exception e) {
					logger.warn("Unable to cleanup Py.", e);
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error in writing Py script for execution.", e);
		} finally {

			// Cleanup
			preScriptFile.delete();
			scriptFile.delete();
		}
	}

	// overloading the run script method to pass in the user map

	public String runScript(Map<String, StringBuffer> appMap, String script, Insight in) {

		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;

		if (this.insight != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT = '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = ", ROOT";

			if (this.insight.isSavedInsight()) {
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

		// get the custom var String
		String varFolderAssignment = "";
		if (appMap != null && appMap.containsKey("PY_VAR_STRING"))
			varFolderAssignment = appMap.get("PY_VAR_STRING").toString();

		// flatten the FolderAssignment string
		varFolderAssignment = varFolderAssignment.replaceAll("[\\r\\n]+", ";");

		String assignmentString = insightRootAssignment + appRootAssignment + userRootAssignment + varFolderAssignment;

		// String assignmentScript = getAssignments(appMap);
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
		if (varFolderAssignment != null && varFolderAssignment.length() > 0) {
			output = cleanCustomVar(output, appMap);
		}

		// Successful case
		return output;
	}

	/**
	 * Run the script By default return the first script passed in use the Executor
	 * to grab the specific code portion if running multiple
	 * 
	 * @param script
	 * @return
	 */
	public synchronized Object runScript(String script) {
		this.pt.command = new String[] { script };
		Object monitor = this.pt.getMonitor();
		Object response = null;
		synchronized (monitor) {
			try {
				monitor.notify();
				monitor.wait(4000);
			} catch (Exception ignored) {

			}
			/*
			 * if(script.length == 1) { response = this.pt.response.get(script[0]); } else {
			 * response = this.pt.response; }
			 */
			response = this.pt.response;
		}

		return ((Hashtable) response).get(script);
	}

	// the output pragma here is not useful. This is purely done so as to avoid
	// copying over all the other methods to TCPPyTranslator
	// and to avoid casting everywhere
	public synchronized Object runScript(String script, String outputPragma) {
		this.pt.command = new String[] { script };
		Object monitor = this.pt.getMonitor();
		Object response = null;
		synchronized (monitor) {
			try {
				monitor.notify();
				monitor.wait(4000);
			} catch (Exception ignored) {

			}
			/*
			 * if(script.length == 1) { response = this.pt.response.get(script[0]); } else {
			 * response = this.pt.response; }
			 */
			response = this.pt.response;
		}

		return ((Hashtable) response).get(script);
	}

	protected String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder("");
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++) {
			if(script[lineIndex] != null) {
				retString.append(script[lineIndex]).append("\n");
			}
		}
		return retString.toString();
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	// make the custom var String
	private String cleanCustomVar(String output, Map<String, StringBuffer> appMap) {
		Iterator<String> varIt = appMap.keySet().iterator();

		while (varIt.hasNext()) {
			// get this key
			String thisKey = varIt.next();
			String thisVal = appMap.get(thisKey).toString();

			output = output.replace(thisVal, thisKey);
		}

		return output;
	}

	// this becomes an issue on windows where it only consumes specific encoding
	public String getCurEncoding() {
		if (curEncoding == null) {
			curEncoding = runPyAndReturnOutput("print(sys.stdout.encoding)");
		}
		return curEncoding;
	}
	
	 /* This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "list(" + frameName + ".columns)";
		ArrayList<String> colNames = (ArrayList<String>) runScript(script);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	 }

//	public static void main(String [] args) {
//		DIHelper helper = DIHelper.getInstance();
//		Properties prop = new Properties();
//		try {
//			prop.load(new FileInputStream("c:/users/pkapaleeswaran/workspacej3/MonolithDev5/RDF_Map_web.prop"));
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		helper.setCoreProp(prop);
//	
//		PyTranslator py = new PyTranslator();
//		PyExecutorThread pt = new PyExecutorThread();
//		pt.start();
//		py.pt = pt;
//		String command = "print('Hello World')" + 
//						  "\n" +
//						  "print('World Hello')" + 
//						  "\n" + 
//						  "a = 2" +
//						  "\n" +
//						  "a" +
//						  "\n" +
//						  "if (a ==2):" +
//						  "\n" + 
//						  "   print('a is 2')";
//		
//		
//		//py.runPyAndReturnOutput("print('Hello World')\nprint('world hello')");
//		String output = py.runPyAndReturnOutput(command);
//		System.out.println("Output >> " + output);
//	}

	
	public synchronized Object runScript(String script, Insight insight) 
	{
		ErrorSenderThread est = null;
		String payload = script;
		if(insight != null)
		{
			est = new ErrorSenderThread();
			est.setInsight(insight);
	
			// write the file to create
			// for now let it be
			String file = Utility.getRandomString(5);
			makeTempFolder(insight.getInsightFolder());
			String pyTemp  = insight.getInsightFolder() + "/Py/Temp";
			file = pyTemp + "/" + file;
			file = file.replace("\\", "/");
			
			script = script.replace("\"", "'");
			payload = "smssutil.runwrappereval_return(\"" + script + "\", '" + file + "', '" + file + "', globals())";
			est.setFile(file);
			est.start();
		}
		
		Object retObject = runScript(payload);
		
		if(insight != null)
		{
			est.stopSession();
		}
		
		return retObject;
		
	}
	
	public void makeTempFolder(String baseFolder)
	{
		String pyTemp  = baseFolder + "/Py/Temp";
		
		File pyTempF = new File(pyTemp);
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
			pyTempF.setExecutable(true);
			pyTempF.setReadable(true);
			pyTempF.setReadable(true);
		}

		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			if (this.insight.getUser() != null) {
				this.insight.getUser().getUserMountHelper().mountFolder(pyTemp, pyTemp, false);
			}
		}

	}

}
