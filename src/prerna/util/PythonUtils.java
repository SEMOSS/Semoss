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
import org.quartz.CronExpression;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import com.google.common.base.Strings;
import com.google.common.net.InternetDomainName;
import com.google.gson.GsonBuilder;
import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;

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
import prerna.om.Insight;
import prerna.ds.py.PyTranslator;
/**
 * The PythonUtilities class contains functions related to the Python logic in SEMOSS.
 * Eventually should combine this with prerna.ds.py.PyUtils but we need to flush out legacy logic from that file.
 * Working on making breaking out some of the main Utility.java file to make it smaller
 */

public final class PythonUtils {
	
	private static final Logger classLogger = LogManager.getLogger(PythonUtils.class);
	
	public static String getUserVenvPath(Insight insight) {
		String venvPath = insight.getUser().tempInsightDir;
		if (venvPath != null && !venvPath.isEmpty()) {
			return venvPath = Utility.normalizePath(venvPath + "/venv");
			
		} else {
			classLogger.error("The Py server directory is not valid. Python may not be instantiated yet");
			return null;
		}
	}
	
	public static String[] getVenvCreationCmd() {
	    if (SystemUtils.IS_OS_WINDOWS) {
	        return new String[]{"python", "-m", "venv"};
	    } else {
	        return new String[]{"python3", "-m", "venv"};
	    }
	}
	
    public static String getVenvActivationCmd(String venvPath) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return venvPath + "\\Scripts\\activate";
        } else {
            return venvPath + "/bin/activate";
        }
    }
    
    public static String[] getPipInstallCmd(String library) {
        if (SystemUtils.IS_OS_WINDOWS) {
        	return new String[]{"cmd", "/c", "pip", "install", library};
        } else {
        	return new String[]{"/bin/bash", "-c", "pip install " + library};
        }
    }
    
    // Combining the activation and installation commands to be used in one process builder
    public static String[] getFullCommand(String activationCommand, String[] installCommand) {
        if (SystemUtils.IS_OS_WINDOWS) {
            return new String[]{"cmd", "/c", activationCommand + " && " + String.join(" ", installCommand)};
        } else {
        	return new String[] {"/bin/bash", "-c", ". " + activationCommand + " && " + String.join(" ", installCommand)};
        }
    }
    
    public static String installLibrary(Insight insight, String library) throws IOException, InterruptedException {
        String venvPath = getUserVenvPath(insight);
        String activationCommand = getVenvActivationCmd(venvPath);
        String[] installCommand = getPipInstallCmd(library);
        
        // Combine activation and installation command
        String[] combinedCommand = getFullCommand(activationCommand, installCommand); 

        ProcessBuilder pb = new ProcessBuilder(combinedCommand[0], combinedCommand[1], combinedCommand[2]);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        int exitCode = process.waitFor();
        if (exitCode == 0) {
            return "Successfully installed library: " + library;
        } else {
            output.append("Failed to install library ").append(library).append(", exit code: ").append(exitCode);
            return output.toString();
        }
    }


	// This method creates a Python virtual environment to a given path
	public static void createVirtualEnv(String venvPath) throws IOException, InterruptedException {
		String[] creationCommand = getVenvCreationCmd();
	    ProcessBuilder pb = new ProcessBuilder(creationCommand[0], creationCommand[1], creationCommand[2], venvPath);
	    pb.redirectErrorStream(true);
	    Process process = pb.start();
	    
	    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
	        String line;
	        while ((line = reader.readLine()) != null) {
	            System.out.println(line);
	        }
	    }
	    
	    int exitCode = process.waitFor();
	    if (exitCode != 0) {
	        throw new IOException("Failed to create virtual environment, exit code: " + exitCode);
	    }
	}

	// This is the method that is being used to start our Py server. I don't know when we would ever use startTCPServerNativePyChroot(), startTCPServer() or startTCPServerChroot(). These may very well be legacy now.
	public static Object [] startTCPServerNativePy(String insightFolder, String port, String py, String timeout, String loggerLevel) {
	String prefix = "";
	Process thisProcess = null;
	// Our insight cache folder
	String finalDir = insightFolder.replace("\\", "/");
	try {
		
		// If the path to our main python distribution was not passed through the params we search for it using the RDF Map
		if (py == null || py.isEmpty()) {
			py = System.getenv(Settings.PYTHONHOME);
			if(py == null) {
				py = DIHelper.getInstance().getProperty(Settings.PYTHONHOME);
			}
			// Can we get rid of these now? 
			if(py == null) {
				System.getenv(Settings.PY_HOME);
			}
			if (py == null) {
				py = DIHelper.getInstance().getProperty(Settings.PY_HOME);
			}
			if(py == null) {
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
		String userVenvPath = finalDir + "/venv";

		try {
			createVirtualEnv(userVenvPath);
		} catch (InterruptedException ie) {
			classLogger.info("FAILED TO CREATE USER VIRTUAL ENV!");
			classLogger.error(Constants.STACKTRACE, ie);
		}

		// Path to our BE Python directory
		String pyBase = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
		pyBase = pyBase.replace("\\", "/");
		String gaasServer = pyBase + "/gaas_tcp_socket_server.py";

		prefix = Utility.getRandomString(5);
		prefix = "p_"+ prefix;
		
		String outputFile = finalDir + "/console.txt";
		
        // Path to our main Python distribution sitepackages directory
        String mainLibPath = Utility.getDIHelperProperty("PYTHON_SITEPACKAGES");
        mainLibPath = mainLibPath.replace("\\", "/");
        // Path to our new virtual environment sitepackage directory
        String userLibPath = userVenvPath + "\\Lib\\site-packages";
        userLibPath = userLibPath.replace("\\", "/");

        // Combining the paths from our main python distribution and our virtual env so we can pull libraries from both
        String combinedPythonPath = mainLibPath + ";" + userLibPath;
        classLogger.info("The combined python path is: " + combinedPythonPath);
		
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
		
		// do I need this ?
		//String[] starterFile = writeStarterFile(commands, finalDir);
		ProcessBuilder pb = new ProcessBuilder(commands);
		ProcessBuilder.Redirect redirector = ProcessBuilder.Redirect.to(new File(outputFile));
		pb.redirectError(redirector);
		pb.redirectOutput(redirector);
		pb.environment().put("PYTHONPATH", combinedPythonPath);
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
}
