package prerna.ds.py;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyTranslator 
{
	
	// this will start to become the main interfacing class for everything related to python
	// this is the equivalent of doing RTranslator on the R end
	
	// this is the default all of the pandas stuff would use
	
	// sets the insight
	Insight insight  = null;
	
	Logger logger = null;

	PyExecutorThread pt = null;
	
	Map <Object, Object> responseCache = new HashMap<Object, Object>();
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
	
	public PyTranslator()
	{
		//startDisruptor();
		System.out.println("Py Translator created");
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
		//this.outBuffer = pt.getBuffer();
	}
	
	public PyExecutorThread getPy() {
		return this.pt;
		//this.outBuffer = pt.getBuffer();
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
		Long x = getLong(script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(String script) {
		Long x = (Long) runScript(script);
		return x;
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
	
	protected synchronized Hashtable executePyDirect(String...script)
	{
		if(this.pt == null)
			this.pt = insight.getPy();
	
		Object monitor = pt.getMonitor();
		synchronized(monitor)
		{
			pt.command = script;
			monitor.notify();
			try{
				monitor.wait();
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
			logger.info("Completed processing");
		}
		return pt.response;
	}
	
	
	protected synchronized void executeEmptyPyDirect(String script)
	{
		if(this.pt == null)
			this.pt = insight.getPy();
	
		Object monitor = pt.getMonitor();
		synchronized(monitor)
		{
			pt.command = new String[]{script};
			monitor.notify();
			try{
				monitor.wait();
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
			logger.info("Completed processing");
			
		}
		
	}

	protected void executeEmptyPyDirect2(String script, String file)
	{
		if(this.pt == null)
			this.pt = insight.getPy();
	
		Object monitor = pt.getMonitor();
		String driverMonitor = insight.getInsightId();
		// I need to go into a while true loop here
		// I need to keep checking to see if the thread is still running
		// if so loop
		File daFile = new File(file);
		int marker = 0;
		try {
			synchronized(monitor)
			{
					pt.command = new String[]{script};
					pt.setDriverMonitor(driverMonitor);
					// keep notifying until the thread has started
					//while(pt.curState == ThreadState.wait)
						monitor.notify();
						//monitor.wait();
			}

			// wait for the file
			//waitFileCreate(file);
			
			while(!daFile.exists())
			{
				try{
					Thread.sleep(100);
					//monitor.notifyAll();
				}catch(Exception ex)
				{
					ex.printStackTrace();
				}
				daFile = new File(file);
			}
			
			// open the file in read mode
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			long offset = raf.getFilePointer();
			int count = 0;
			int sleepTime = 200;
			int sleepMax = 3000;
			int numLines = 0;
			do{
				// release the lock
				
				//monitor.wait();
				long dataAvailable = daFile.length();
				if(offset < dataAvailable)
				{
					raf.seek(offset);
					String line = raf.readLine();
		            do
		            {
		              line = raf.readLine();
		              //numLines = numLines + 1;
		              if(line != null)
		            	  logger.info(Utility.cleanLogString(line));
		            }while( line != null );
		            // if there were more than 3 lines that assimilated in 200 milliseconds increase sleep time ?
		            if(count %3 == 0)
		            	sleepTime = sleepTime + 100;
		            if(sleepTime > sleepMax)
		            	sleepTime = sleepMax;
			        offset = raf.getFilePointer();
				}
				// sleep for sleepTime or until he pythread informs us
				synchronized(driverMonitor)
				{
					driverMonitor.wait(sleepTime);
				}
			}while(pt.curState != ThreadState.wait);
			logger.info("Completed processing");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public synchronized void runEmptyPy(String...script)
	{
		// get the insight folder 
		// create a teamp to write the script file
		String rootPath = null;
		String pyTemp = null;
		String addRootVariable = "";
		if(this.insight != null) {
			rootPath = this.insight.getInsightFolder().replace('\\', '/');
			pyTemp = rootPath + "/py/Temp/";
			addRootVariable = "ROOT <- '" + rootPath + "';";
			String removeRootVar = "ROOT";
		} else {
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}
		
		File pyTempF = new File(pyTemp);
		if(!pyTempF.exists()) {
			pyTempF.mkdirs();
		}

		String scriptFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp + scriptFileName + ".py";
		File scriptFile = new File(scriptPath);
		
		try {
			String finalScript = convertArrayToString(script);
			FileUtils.writeStringToFile(scriptFile, finalScript);
			
			// the wrapper needs to be run now
			//executePyDirect("runwrapper(" + scriptPath + "," + outPath + "," + outPath + ")");
			//executePyDirect("smssutil.run_empty_wrapper(\"" + scriptPath + "\", globals())");
			// changing this to runscript
			runScript("smssutil.run_empty_wrapper(\"" + scriptPath + "\", globals())");
			
		} catch (IOException e1) {
			System.out.println("Error in writing Py script for execution!");
			e1.printStackTrace();
		}
	}
	public synchronized String runPyAndReturnOutput(String...inscript)
	{
		return runPyAndReturnOutput(null, inscript);
	}
	
	
	public synchronized String runPyAndReturnOutput(Map <String, StringBuffer> appMap, String...inscript)
	{
		// Clean the script
		String script = convertArrayToString(inscript);
		script = script.trim();
		
		// find if the script is simple
		boolean multi = (inscript.length > 1 || script.contains("\n")) || script.contains("=") || (script.contains(".") && script.endsWith("()")) && !script.equals("dir()"); 
		
		
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
		if(this.insight != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT = '" + insightRootPath + "';";
			removePathVariables = ", ROOT";
			
			if(this.insight.isSavedInsight()) {
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
			} catch(Exception ignore) {
				// ignore
			}
			
			pyTemp = insightRootPath + "/Py/Temp/";
		} else {
			pyTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/Py/Temp/").replace('\\', '/');
		}
		
		// get the custom var String
		String varFolderAssignment = "";
		if(appMap != null && appMap.containsKey("PY_VAR_STRING"))
			varFolderAssignment = appMap.get("PY_VAR_STRING").toString();		
		
		File pyTempF = new File(pyTemp);
		if(!pyTempF.exists()) {
			pyTempF.mkdirs();
		}
		
		
		
		String pyFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp +  pyFileName + ".py";
		File scriptFile = new File(scriptPath);	
		String outputPath = pyTemp + pyFileName + ".txt";
		File outputFile = new File(outputPath);

		multi = true;
		
		if(script.startsWith("@"))
		{
			multi = false;
		}
		
		// attempt to put it into environment
		script = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment + "\n" + varFolderAssignment + "\n" + script;

		if(multi)
		{
			// Try writing the script to a file
			try {
				FileUtils.writeStringToFile(scriptFile, script);
	
				// check packages
				//checkPackages(script);
	
				// Try running the script, which saves the output to a file
				 // TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R (later)
				RuntimeException error = null;
				try {
					executeEmptyPyDirect("smssutil.runwrapper(\"" + scriptPath + "\", \"" + outputPath + "\", \"" + outputPath + "\", globals())");
					//executeEmptyPyDirect2("smssutil.runwrapper(\"" + scriptPath + "\", \"" + outputPath + "\", \"" + outputPath + "\", globals())", outputPath);
				} catch (RuntimeException e) {
					e.printStackTrace();
					error = e; // Save the error so we can report it
				}
				
				// Finally, read the output and return, or throw the appropriate error
				try {
					String output = FileUtils.readFileToString(outputFile).trim();
					// Error cases
					
					// clean up the output
					if(userRootPath != null && output.contains(userRootPath)) {
						output = output.replace(userRootPath, "$USER_IF");
					}
					if(appRootPath != null && output.contains(appRootPath)) {
						output = output.replace(appRootPath, "$APP_IF");
					}
					if(insightRootPath != null && output.contains(insightRootPath)) {
						output = output.replace(insightRootPath, "$IF");
					}
					if(varFolderAssignment != null && varFolderAssignment.length() > 0)
					{
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
						//this.executeEmptyR("rm(" + randomVariable + removePathVariables + ");");
						//this.executeEmptyR("gc();"); // Garbage collection
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
		}
		else
		{
			String finalScript = convertArrayToString(inscript);
			finalScript = finalScript.replace("@", "");
			Hashtable response = executePyDirect(finalScript);
			return response.get(finalScript) + "";
		}
		
	}
	
	/**
	 * Run the script
	 * By default return the first script passed in
	 * use the Executor to grab the specific code portion if running multiple
	 * @param script
	 * @return
	 */
	public synchronized Object runScript(String script) {
		this.pt.command = new String[] {script};
		Object monitor = this.pt.getMonitor();
		Object response = null;
		synchronized(monitor) {
			try {
				monitor.notify();
				monitor.wait(4000);
			} catch (Exception ignored) {
				
			}
			/*if(script.length == 1) {
				response = this.pt.response.get(script[0]);
			} else {
				response = this.pt.response;
			}*/
			response = this.pt.response;
		}
		
		return ((Hashtable)response).get(script);
	}


	
	private String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder("");
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++)
			retString.append(script[lineIndex]).append("\n");
		return retString.toString();
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	// make the custom var String
	private String cleanCustomVar(String output, Map <String, StringBuffer> appMap)
	{
		Iterator <String> varIt = appMap.keySet().iterator();

		while(varIt.hasNext())
		{
			// get this key
			String thisKey = varIt.next();
			String thisVal = appMap.get(thisKey).toString();
			
			output = output.replace(thisVal, thisKey);			
		}
		
		return output;
	}

	
	public static void main(String [] args)
	{
		DIHelper helper = DIHelper.getInstance();
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("c:/users/pkapaleeswaran/workspacej3/MonolithDev5/RDF_Map_web.prop"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		helper.setCoreProp(prop);
	
		PyTranslator py = new PyTranslator();
		PyExecutorThread pt = new PyExecutorThread();
		pt.start();
		py.pt = pt;
		String command = "print('Hello World')" + 
						  "\n" +
						  "print('World Hello')" + 
						  "\n" + 
						  "a = 2" +
						  "\n" +
						  "a" +
						  "\n" +
						  "if (a ==2):" +
						  "\n" + 
						  "   print('a is 2')";
		
		
		//py.runPyAndReturnOutput("print('Hello World')\nprint('world hello')");
		String output = py.runPyAndReturnOutput(command);
		System.out.println("Output >> " + output);
	}
	
}
