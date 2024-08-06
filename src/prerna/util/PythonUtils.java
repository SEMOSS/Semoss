package prerna.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

import prerna.ds.py.PyUtils;

/**
 * The PythonUtilities class contains functions related to the Python logic in SEMOSS.
 * Eventually should combine this with prerna.ds.py.PyUtils but we need to flush out legacy logic from that file.
 * Working on making breaking out some of the main Utility.java file to make it smaller
 */

public final class PythonUtils {
	
	private static final Logger classLogger = LogManager.getLogger(PythonUtils.class);
	
	// This is the method that is being used to start our Py server. I don't know when we would ever use startTCPServerNativePyChroot(), startTCPServer() or startTCPServerChroot(). These may very well be legacy now.
	public static Object [] startTCPServerNativePy(String insightFolder, String port, String py, String timeout, String loggerLevel) {
	String prefix = "";
	Process thisProcess = null;
	String finalDir = insightFolder.replace("\\", "/");
	try {
		
		// If the path to our main python distribution was not passed through the params we search for it using the RDF Map
		if (py == null || py.isEmpty()) {
		    py = System.getenv(Settings.PYTHONHOME);
		    if (py == null) {
		        py = DIHelper.getInstance().getProperty(Settings.PYTHONHOME);
		    }
		    if (py == null) {
		        if (!System.getenv(Settings.PY_HOME).isEmpty() || !DIHelper.getInstance().getProperty(Settings.PY_HOME).isEmpty()) {
		            throw new NullPointerException("PY_HOME is deprecated. Please set PYTHONHOME");
		        }
		        throw new NullPointerException("Must define python home");
		    }
		}
		
		// Appending the executable file based on the OS
		if (SystemUtils.IS_OS_WINDOWS) {
			py = py + "/python.exe";
		} else {
			py = py + "/bin/python3";
		}
		
		py = py.replace("\\", "/");
		classLogger.info("The main python executable being used is: " + py);
		
		// Path to our virtual environment inside our insight cache folder 
		String userVenvPath = Utility.normalizePath(finalDir + "\\venv\\Lib\\site-packages");
		classLogger.info("The userVenvPath is: " + userVenvPath);

		// Path to our BE Python directory
		String pyBase = Utility.normalizePath(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER);
		String gaasServer = Utility.normalizePath(pyBase + "/gaas_tcp_socket_server.py");

		prefix = Utility.getRandomString(5);
		prefix = "p_"+ prefix;
		
		String outputFile = Utility.normalizePath(finalDir + "\\console.txt");
		
		String[] commands = new String[] {py, gaasServer, "--port", port, "--max_count", "1", "--py_folder", pyBase, "--insight_folder", finalDir, "--prefix", prefix, "--timeout", timeout, "--logger_level" , loggerLevel};
			
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
		
		ProcessBuilder pb = new ProcessBuilder(commands);
		ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(new File(outputFile));
		pb.redirectError(redirector);
		pb.redirectOutput(redirector);
		pb.environment().put("PYTHONPATH", userVenvPath);
		Process p = pb.start();
		try {
			p.waitFor(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			classLogger.error(Constants.STACKTRACE, ie);
		}
		classLogger.info("came out of the waiting for process");
		if (!p.isAlive()) {
			// if it crashed here, then the outputFile will contain the error. Read file and send error back
			// it should not contain anything else since we are trying to start the server here
        	BufferedReader reader = new BufferedReader(new FileReader(outputFile));
			StringBuilder errorMsg = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null ) {
                // get the runtime error
            	if (line.startsWith("Traceback")) {
            		errorMsg.append(line).append("\n");
            		while ((line = reader.readLine()) != null ) {
            			errorMsg.append(line).append("\n");
            		}
            	}
            }
            reader.close();
            if (!errorMsg.toString().isEmpty())
            	throw new IllegalStateException(errorMsg.toString());
		}
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
	
	// Various checks to make sure the user is setup to run Python
	public static void verifyPyCapabilities() {
	    String disable_terminal = DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);

	    if (disable_terminal != null && !disable_terminal.isEmpty()) {
	        if (Boolean.parseBoolean(disable_terminal)) {
	            throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
	        }
	    }

	    if (!PyUtils.pyEnabled()) {
	        throw new IllegalArgumentException("Python is not enabled to use the following command");
	    }

	    // Check if py terminal is disabled
	    String disable_py_terminal = DIHelper.getInstance().getProperty(Constants.DISABLE_PY_TERMINAL);
	    if (disable_py_terminal != null && !disable_py_terminal.isEmpty()) {
	        if (Boolean.parseBoolean(disable_py_terminal)) {
	            throw new IllegalArgumentException("Python terminal has been disabled.");
	        }
	    }
	}
}
