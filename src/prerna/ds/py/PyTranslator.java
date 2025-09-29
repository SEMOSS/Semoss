package prerna.ds.py;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.ThreadContext;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;

public class PyTranslator {

	static Map<String, SemossDataType> pyS = new Hashtable<String, SemossDataType>();
	static {
		pyS.put("object", SemossDataType.STRING);
		pyS.put("category", SemossDataType.STRING);
		pyS.put("int64", SemossDataType.INT);
		pyS.put("float64", SemossDataType.DOUBLE);
		pyS.put("datetime64", SemossDataType.DATE);
		pyS.put("datetime64[ns]", SemossDataType.TIMESTAMP);
	}

	public static String curEncoding = null;

	private SocketClient sc = null;
	private Insight globalStoreInsight = null;

	/**
	 * 
	 * @param sc
	 * @param this.globalStoreInsight
	 */
	public PyTranslator(SocketClient sc, Insight globalStoreInsight) {
		this.sc = sc;
		this.globalStoreInsight = globalStoreInsight;
	}

	public SocketClient getSocketClient() {
		return this.sc;
	}

	public Insight getGlobalStoreInsight() {
		return this.globalStoreInsight;
	}

	public void setSocketClient(SocketClient sc) {
		this.sc = sc;
	}

	public SemossDataType convertDataType(String pDataType) {
		return pyS.get(pDataType);
	}

	/**
	 * 
	 * @return
	 */
	public String getCurEncoding() {
		if (curEncoding == null) {
			curEncoding = (String) transportScript(null, "sys.stdout.encoding");
		}
		return curEncoding;
	}

	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(String script) {
		return (List<Object>) transportScript(null, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(String script) {
		List<String> val = (List<String>) transportScript(null, script);
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
		Boolean x = (Boolean) transportScript(null, script);
		return x.booleanValue();
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(String script) {
		Number x = (Number) transportScript(null, script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(String script) {
		Number x = (Number) transportScript(null, script);
		return x.longValue();
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(String script) {
		Number x = (Number) transportScript(null, script);
		return x.doubleValue();
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(String script) {
		return (String) transportScript(null, script);
	}

	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "list(" + frameName + ".columns)";
		List<String> colNames = (List<String>) transportScript(null, script);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param script
	 */
	public void runEmptyPy(String... script) {
		this.transportScript(null, convertArrayToString(script));
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param script
	 */
	public Object runDirectPy(String... script) {
		return this.transportScript(null, convertArrayToString(script));
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @param executionInsight If we have a User invoking an engine python process
	 *                         The engine python process has its own unique insight
	 *                         for variable encapsulation However, we need to know
	 *                         from what insight is the user invoking this request
	 *                         So that if the engine is making a call back/reactor
	 *                         request It knows which User invoked for security
	 *                         permissions
	 * @param script
	 * @return
	 */
	public Object runDirectPy(Insight executionInsight, String... script) {
		return this.transportScript(executionInsight, convertArrayToString(script));
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @param script
	 * @return
	 */
	public Object runScript(String... script) {
		return this.executePyWithDefualtVars(null, convertArrayToString(script));
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @param executionInsight If we have a User invoking an engine python process
	 *                         The engine python process has its own unique insight
	 *                         for variable encapsulation However, we need to know
	 *                         from what insight is the user invoking this request
	 *                         So that if the engine is making a call back/reactor
	 *                         request It knows which User invoked for security
	 *                         permissions
	 * @param script
	 * @return
	 */
	public Object runScript(Insight executionInsight, String... script) {
		return this.executePyWithDefualtVars(executionInsight, convertArrayToString(script));
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the
	 * execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runDirectPy(String...)}
	 *             instead.
	 * @param script
	 * @param this.globalStoreInsight
	 * @return
	 */
	@Deprecated
	public Object runSmssWrapperEval(String script) {
		return this.transportScript(null, script);
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runScript(String...)}
	 *             instead.
	 * @param script
	 * @return
	 */
	@Deprecated
	public String runPyAndReturnOutput(String... script) {
		return this.executePyWithDefualtVars(null, convertArrayToString(script)) + "";
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * 
	 * @deprecated This method is deprecated. Use {@link #runScript(String...)}
	 *             instead.
	 * @param script
	 * @return
	 */
	@Deprecated
	public String runSingle(String... script) {
		return this.executePyWithDefualtVars(null, convertArrayToString(script)) + "";
	}

	/**
	 * 
	 * @param executionInsight
	 * @param script
	 * @return
	 */
	private Object executePyWithDefualtVars(Insight executionInsight, String script) {
		String[] paths = getDefaultPaths(this.globalStoreInsight);
		StringBuilder pathVars = generateDefaultVars(paths);
		transportScript(executionInsight, pathVars.toString());

		Object output = transportScript(executionInsight, script);
		if (output instanceof String) {
			String strOutput = (String) output;
			// clean up the output
			if (paths[0] != null && strOutput.contains(paths[0])) {
				strOutput = strOutput.replace(paths[0], "$IF");
			}
			if (paths[1] != null && strOutput.contains(paths[1])) {
				strOutput = strOutput.replace(paths[1], "$APP_IF");
			}
			if (paths[2] != null && strOutput.contains(paths[2])) {
				strOutput = strOutput.replace(paths[2], "$USER_IF");
			}
			return strOutput;
		}
		return output;
	}

	/**
	 * 
	 * @param defaultPaths
	 * @return
	 */
	private StringBuilder generateDefaultVars(String[] defaultPaths) {
		StringBuilder script = new StringBuilder();
		String[] pathVars = new String[] { "ROOT", "APP_ROOT", "USER_ROOT" };
		for (int i = 0; i < pathVars.length; i++) {
			if (defaultPaths[i] != null && !(defaultPaths[i] = defaultPaths[i].trim()).isEmpty()) {
				script.append(pathVars[i]).append(" = '").append(defaultPaths[i]).append("'\n");
			}
		}

		return script;
	}

	/**
	 * 
	 * @param insight
	 * @return
	 */
	private String[] getDefaultPaths(Insight insight) {
		String insightPath = insight.getInsightFolder().replace('\\', '/');
		String appPath = null;
		String userPath = null;

		// context project takes precedence
		if (insight.getContextProjectId() != null) {
			appPath = AssetUtility.getProjectAssetsFolder(insight.getContextProjectName(),
					insight.getContextProjectId());
			appPath = appPath.replace('\\', '/');
		} else if (insight.isSavedInsight()) {
			appPath = insight.getAppFolder();
			appPath = appPath.replace('\\', '/');
		}
		try {
			userPath = AssetUtility.getRootFolderPath(insight, AssetUtility.USER_SPACE_KEY, false);
			userPath = userPath.replace('\\', '/');
		} catch (Exception ignore) {
			// ignore
		}

		return new String[] { insightPath, appPath, userPath };
	}

	/**
	 * 
	 * @param executionInsight
	 * @param script
	 * @return
	 */
	private Object transportScript(Insight executionInsight, String script) {
		String methodName = new Object() {
		}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.methodName = methodName;
		ps.payload = new Object[] { script };
		ps.payloadClasses = new Class[] { String.class };
		ps.longRunning = true;
		// we always need an insight
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (executionInsight != null) {
			ps.executionInsightId = executionInsight.getInsightId();
		}

		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
			return ps.payload[0];
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * 
	 */
	public void clearInsightGlobals() {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.INSIGHT;
		ps.payload = new Object[] { "CLEAR_NON_MODULE_GLOBALS" };
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * 
	 */
	public void removeInsightGlobals() {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.INSIGHT;
		ps.payload = new Object[] { "REMOVE_INSIGHT_GLOBALS" };
		ps.insightId = this.globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		ps.sessionId = ThreadStore.getSessionId();
		ps.mdc = ThreadContext.getImmutableContext();
		if (sc.isConnected()) {
			ps = (PayloadStruct) sc.executeCommand(ps);
			if (ps == null) {
				throw new SemossPixelException("Received a null PayloadStruct response");
			}
			if (ps.ex != null) {
				throw new SemossPixelException(ps.ex);
			}
		} else {
			throw new SemossPixelException(
					"Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	/**
	 * 
	 * @param script
	 * @return
	 */
	private String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder();
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++) {
			if (script[lineIndex] != null) {
				retString.append(script[lineIndex]).append("\n");
			}
		}
		return retString.toString();
	}
	
	public Object runPyMethodFromFile(String filePath, String functionName, List<String> argsList) {
		
		String projectId = this.globalStoreInsight.getContextProjectId();
	    if (projectId == null) {
	    	projectId = this.globalStoreInsight.getProjectId();
	    }
	    
	    String appFolder = AssetUtility.getProjectAssetsFolder(projectId);
	    filePath = appFolder + "/" + filePath;
	    filePath = filePath.replace("\\", "/");
	    String[] sourceFileSplit = filePath.split("/");
	    String sourceFile = sourceFileSplit[sourceFileSplit.length - 1];
	    
	    String moduleName = sourceFile.replace(".py", "");
	    
	    String args = "";
	    if (argsList != null && !argsList.isEmpty()) {
	      args = String.join(", ", argsList);
	    }
	    
	    String commands =
            "import sys\n"
                + "sys.path.append(\""
                + filePath
                + "\")\n"
                + "from "
                + moduleName
                + " import "
                + functionName
                + "\n"
                + functionName
                + "("
                + args
                + ")\n";

        Object pyResponse = runDirectPy(this.globalStoreInsight, commands);
	    
		return pyResponse;
	}
	
	public String loadPythonModuleFromFile(String fileLocation, String functionName, List<String> argsList, String space) {
		String filePath = UploadInputUtility.getFilePath(this.globalStoreInsight, fileLocation, space);
		
		String appFolder = null;
		if (space != null) {
			appFolder = AssetUtility.getProjectAssetsFolder(space) + "/" + Constants.PY_BASE_FOLDER;
			appFolder = appFolder.replace("\\", "/");
		}
		
		String alias = "pyModule";
		
		try {
			if(appFolder != null)
			{
				String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='" + filePath +"', search='" + appFolder + "')";
				this.globalStoreInsight.getPyTranslator().runScript(script);
			}
			else
			{
				String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='" + filePath +"', search=None)";
				this.globalStoreInsight.getPyTranslator().runScript(script);
				
			}
		} catch (Exception e) {
			throw new SemossPixelException("Unable to load python file as module");
		}
		
		return alias;
	}

}