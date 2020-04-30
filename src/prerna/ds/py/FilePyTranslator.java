package prerna.ds.py;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.pyserve.Commandeer;
import prerna.pyserve.NettyClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FilePyTranslator extends PyTranslator {

	// this will start to become the main interfacing class for everything related
	// to python
	// this is the equivalent of doing RTranslator on the R end

	// this is the default all of the pandas stuff would use
	private static final String STACKTRACE = "StackTrace: ";

	// sets the insight
	Insight insight = null;

	Logger logger = null;

	NettyClient nc = null;
	public String HOST = "127.0.0.1";
	public int port = 6666;

	// TODO need to replace this duplicate code from PandasFrame
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	static Map<String, SemossDataType> pyS = new Hashtable<>();
	static {
		pyS.put("object", SemossDataType.STRING);
		pyS.put("category", SemossDataType.STRING);
		pyS.put("int64", SemossDataType.INT);
		pyS.put("float64", SemossDataType.DOUBLE);
		pyS.put("datetime64", SemossDataType.DATE);
		pyS.put("datetime64[ns]", SemossDataType.TIMESTAMP);
	}

	@Override
	public SemossDataType convertDataType(String pDataType) {
		return pyS.get(pDataType);
	}
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	// sets the insight
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	@Override
	public List<Object> getList(String script) {
		return (List<Object>) runScript(script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	@Override
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
	@Override
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
	@Override
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
	@Override
	public int getInt(String script) {
		Long x = getLong(script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	@Override
	public Long getLong(String script) {
		return (Long) runScript(script);
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	@Override
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
	@Override
	public String getString(String script) {
		return (String) runScript(script);
	}

	@Override
	protected Hashtable executePyDirect(String... script) {
		return executePyDirectFile(script);
	}

	@Override
	protected void executeEmptyPyDirect(String script) {
		executePyDirectFile(script);
	}

	@Override
	public void runEmptyPy(String... script) {
		// get the insight folder
		// create a teamp to write the script file

		// run py direct here
		runScriptFilePy(script);

		// the wrapper needs to be run now
		// executePyDirect("runwrapper(" + scriptPath + "," + outPath + "," + outPath +
		// ")");
		// executePyDirect("smssutil.run_empty_wrapper(\"" + scriptPath + "\",
		// globals())");
		// changing this to runscript
		// runScript("smssutil.run_empty_wrapper(\"" + scriptPath + "\", globals())");
	}

	@Override
	public String runPyAndReturnOutput(String... inscript) {
		// Clean the script
		String script = convertArrayToAString(inscript);
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
			insightRootAssignment = "ROOT = '" + insightRootPath + "';";
			removePathVariables = ", ROOT";

			if (this.insight.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT = '" + appRootPath + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
				userRootPath = userRootPath.replace('\\', '/');
				userRootAssignment = "USER_ROOT = '" + userRootPath + "';";
				removePathVariables += ", USER_ROOT";
			} catch (Exception ignore) {
				// ignore
			}

			pyTemp = insightRootPath + "/Py/Temp/";
		} else {
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}
		File pyTempF = new File(pyTemp);
		if (!pyTempF.exists()) {
			pyTempF.mkdirs();
		}

		String pyFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp + pyFileName + ".py";
		File scriptFile = new File(scriptPath);
		String outputPath = pyTemp + pyFileName + ".txt";
		File outputFile = new File(outputPath);

		// attempt to put it into environment
		script = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment + "\n" + script;
		Object retObj = null;

		if (multi) {
			// Try writing the script to a file
			try {
				FileUtils.writeStringToFile(scriptFile, script);

				// check packages
				// checkPackages(script);

				// Try running the script, which saves the output to a file
				// TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R
				// (later)
				RuntimeException error = null;
				try {
					// just run the script directly here
					retObj = runScriptFilePy(script);

					// executeEmptyPyDirect2("smssutil.runwrapper(\"" + scriptPath + "\", \"" +
					// outputPath + "\", \"" + outputPath + "\", globals())", outputPath);
				} catch (RuntimeException e) {
					logger.error(e.getStackTrace());
					error = e; // Save the error so we can report it
				}

				// Finally, read the output and return, or throw the appropriate error
				try {
					// Error cases
					String output = (String) retObj;
					// clean up the output
					if (userRootPath != null && output != null && output.contains(userRootPath)) {
						output = output.replace(userRootPath, "$USER_IF");
					}
					if (appRootPath != null && output != null && output.contains(appRootPath)) {
						output = output.replace(appRootPath, "$APP_IF");
					}
					if (insightRootPath != null && output != null && output.contains(insightRootPath)) {
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
					outputFile.delete();
					try {
						// this.executeEmptyR("rm(" + randomVariable + removePathVariables + ");");
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
			}
		} else {
			String finalScript = convertArrayToAString(inscript);
			Hashtable response = executePyDirectFile(finalScript);
			return response.get(finalScript) + "";
		}

	}

	/**
	 * Run the script By default return the first script passed in use the Executor
	 * to grab the specific code portion if running multiple
	 * 
	 * @param script
	 * @return
	 */
	public Object runScript(String... script) {
		if (script.length == 1) {
			return runScriptFilePyDirect(script[0]);
		} else {
			return runScriptFilePy(script);
		}
		
		/*
		this.pt.command = script;
		Object monitor = this.pt.getMonitor();
		Object response = null;
		
		// going to track all the options as well here
		if(!script[0].startsWith("smssutil"))
			runScriptFilePy(script);
		
		synchronized(monitor) {
			try {
				monitor.notify();
				monitor.wait(4000);
			} catch (Exception ignored) {
				
			}
			if(script.length == 1) {
				response = this.pt.response.get(script[0]);
			} else {
				response = this.pt.response;
			}
		}
		return response;
		*/
	}

	private String convertArrayToAString(String... script) {
		StringBuilder retString = new StringBuilder("");
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++)
			retString.append(script[lineIndex]).append("\n");
		return retString.toString();
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public static void main(String[] args) {
		DIHelper helper = DIHelper.getInstance();
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("c:/users/pkapaleeswaran/workspacej3/MonolithDev5/RDF_Map_web.prop"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		helper.setCoreProp(prop);

		FilePyTranslator py = new FilePyTranslator();
		String command = "print('Hello World')" + "\n" + "print('World Hello')" + "\n" + "a = 2" + "\n" + "a" + "\n"
				+ "if (a ==2):" + "\n" + "   print('a is 2')";

		// py.runPyAndReturnOutput("print('Hello World')\nprint('world hello')");
		String output = py.runPyAndReturnOutput(command);
		System.out.println("Output >> " + output);
	}

	// ==================================== All the file based stuff goes here
	// ================================================================
	public Object runScriptFilePyDirect(String script) {
		// PY Direct
		script = "PY_DIRECT@@" + script;

		return runScriptFilePy(script);
	}

	public Object runScriptFilePy(String... script) {
		Object output = null;
		try {
			String folderToWatch = insight.getTupleSpace();
			String fileName = this.insight.getInsightId() + "--command--" + this.insight.getCount() + ".py";
			String scriptFile = folderToWatch + "/" + fileName;
			StringBuffer cmdBuffer = new StringBuffer();
			for (int cmdIndex = 0; cmdIndex < script.length; cmdIndex++) {
				cmdBuffer.append(script[cmdIndex]); // .append("\n");
				if (cmdIndex + 1 < script.length)
					cmdBuffer.append("\n");
			}
			File scriptFileF = new File(scriptFile);
			FileUtils.writeStringToFile(scriptFileF, cmdBuffer.toString());

			WatchService watchService = FileSystems.getDefault().newWatchService();
			Path path = Paths.get(folderToWatch);
			WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
					StandardWatchEventKinds.ENTRY_MODIFY);

			WatchKey key;
			Commandeer comm = new Commandeer();
			comm.file = scriptFile;
			Thread commandThread = new Thread(comm);
			commandThread.start();

			// key = watchService.take();
			while ((key = watchService.take()) != null) {
				boolean breakout = false;
				for (WatchEvent<?> event : key.pollEvents()) {
					String file = (String) (event.context().toString());
					Kind kind = event.kind();
					// System.out.println("Event kind:" + kind + ". File affected: " + file + ".");
					// if(kind == StandardWatchEventKinds.ENTRY_CREATE)
					if (kind == StandardWatchEventKinds.OVERFLOW) {
						logger.error("Interesting.. got here " + file);
					}
					
					if (file.endsWith(".completed")) // need to also compare that this is the same file
					{
						// do some checks with the file
						// see if the file is the same starting as the input
						// if it is poke - read the input and say it is taking too much time
						if (file.equalsIgnoreCase("poke.completed")) {
							// this is we poked externally
							output = printOutput(fileName + ".go");
							output = "Command " + output + "  is taking a long time, aborting...";
						} else {
							if (file.contains(fileName)) {
								// System.err.println(">> This is what we should pick.. ");
								// see if the object is there
								Object retObject = readObject(folderToWatch + "/" + file);
								if (retObject != null) {
									// System.err.println("retObject is " + retObject);
									output = retObject;
								} else
									output = printOutput(file);
								breakout = true;
								break;
							}
						}
					}
					// else if()// this is a modify request
					// processComplete(file);
					key.reset();
					// barr.await();
				}
				if (breakout)
					break;
			}
		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error(STACKTRACE, e);
		}

		return output;
	}

	public Object readObject(String file) {
		Object retObject = null;
		file = file.replace("state.completed", "smss_obj");
		File outFile = new File(file);
		if (outFile.exists()) {
			try {
				FileInputStream fis = new FileInputStream(outFile);

				ByteArrayInputStream bis = new ByteArrayInputStream(FileUtils.readFileToByteArray(outFile));

				FSTObjectInput fi = new FSTObjectInput(bis);
				retObject = fi.readObject();

				// ObjectInputStream ois = new ObjectInputStream(fis);
				// retObject = ois.readObject();
				fi.close();
				fis.close();
			} catch (FileNotFoundException e) {
				logger.error(STACKTRACE, e);
			} catch (ClassNotFoundException cnfe) {
				logger.error(STACKTRACE, cnfe);
			} catch (IOException ioe) {
				logger.error(STACKTRACE, ioe);
			}
		}
		return retObject;
	}

	public void runScriptFilePyAsync(String... script) {
		String output = null;
		try {
			String folderToWatch = insight.getTupleSpace();
			String fileName = this.insight.getInsightId() + "--command--" + this.insight.getCount() + ".py";
			String scriptFile = folderToWatch + "/" + fileName;
			StringBuffer cmdBuffer = new StringBuffer();
			for (int cmdIndex = 0; cmdIndex < script.length; cmdBuffer.append(script[cmdIndex])
					.append("\n"), cmdIndex++)
				;
			FileUtils.writeStringToFile(new File(scriptFile), cmdBuffer.toString());

			Commandeer comm = new Commandeer();
			comm.file = scriptFile;
			Thread commandThread = new Thread(comm);
			commandThread.start();

		} catch (IOException ioe) {
			logger.error(STACKTRACE, ioe);
		} catch (Exception e) {
			logger.error(STACKTRACE, e);
		}
	}

	public String printOutput(String file) {
		try {
			file = file.replace(".state.completed", "");
			String command;
			logger.error("Reading from file.. " + file);
			File outputFile = new File(insight.getTupleSpace() + "/" + file + ".output");
			if (outputFile.exists()) {
				command = FileUtils.readFileToString(outputFile).trim();
				// System.out.println("Output >>" + command);
				return command;
			}
			// create a new file now
			new File(insight.getTupleSpace() + "/" + file + ".state.delivered").createNewFile();

		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		}

		return null;
	}

	protected Hashtable executePyDirectFile(String... script) {
		Hashtable response = new Hashtable();
		response.put(script[0], runScriptFilePy(script));
		return response;
	}

	public Object tryNetty(String command) {
		logger.debug(".");
		Object response = insight.nc.executeCommand(command);
		// System.err.println("Got the response back !!!!! WOO HOO " + response);
		return response;
	}

}
