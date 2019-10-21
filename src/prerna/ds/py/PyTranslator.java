package prerna.ds.py;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyTranslator {
	
	// this will start to become the main interfacing class for everything related to python
	// this is the equivalent of doing RTranslator on the R end
	
	// this is the default all of the pandas stuff would use
	
	// sets the insight
	Insight insight  = null;
	
	Logger logger = null;

	PyExecutorThread pt = null;
	
	//sets the insight
	public void setInsight(Insight insight)
	{
		this.insight = insight;
	}
	
	public void setPy(PyExecutorThread pt)
	{
		this.pt = pt;
	}
	
	protected Hashtable executePyDirect(String...script)
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
	
	
	protected void executeEmptyPyDirect(String script)
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
	
	public void runEmptyPy(String...script)
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
		String outPath = pyTemp + scriptFileName + ".out";
		String errPath = pyTemp + scriptFileName + ".err";
		File scriptFile = new File(scriptPath);
		
		try {
			String finalScript = convertArrayToString(script);
			FileUtils.writeStringToFile(scriptFile, finalScript);
			
			// the wrapper needs to be run now
			//executePyDirect("runwrapper(" + scriptPath + "," + outPath + "," + outPath + ")");
			executePyDirect("smssutil.run_empty_wrapper(\"" + scriptPath + "\")");
			
		} catch (IOException e1) {
			System.out.println("Error in writing Py script for execution!");
			e1.printStackTrace();
		}
	}
	
	public String runPyAndReturnOutput(String...inscript)
	{
		// Clean the script
		String script = convertArrayToString(inscript);
		script = script.trim();
		
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
		File pyTempF = new File(pyTemp);
		if(!pyTempF.exists()) {
			pyTempF.mkdirs();
		}
		
		String pyFileName = Utility.getRandomString(12);
		String scriptPath = pyTemp +  pyFileName + ".py";
		File scriptFile = new File(scriptPath);	
		String outputPath = pyTemp + pyFileName + ".txt";
		File outputFile = new File(outputPath);
		
		// attempt to put it into environment
		script = insightRootAssignment + "\n" + appRootAssignment + "\n" + userRootAssignment + "\n" + script;

		// Try writing the script to a file
		try {
			FileUtils.writeStringToFile(scriptFile, script);

			// check packages
			//checkPackages(script);

			// Try running the script, which saves the output to a file
			 // TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R (later)
			RuntimeException error = null;
			try {
				executeEmptyPyDirect("smssutil.runwrapper(\"" + scriptPath + "\", \"" + outputPath + "\", \"" + outputPath + "\")");
			} catch (RuntimeException e) {
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
	
	private String convertArrayToString(String...script)
	{
		StringBuilder retString = new StringBuilder("");
		for(int lineIndex = 0;lineIndex < script.length;lineIndex++)
			retString.append(script[lineIndex]).append("\n");
		return retString.toString();
	}
	
	public void setLogger(Logger logger) {
		this.logger = logger;
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
						  "if (a ==2):" +
						  "\n" + 
						  "   print('a is 2')";
		
		
		//py.runPyAndReturnOutput("print('Hello World')\nprint('world hello')");
		py.runPyAndReturnOutput(command);
	}
	

}
