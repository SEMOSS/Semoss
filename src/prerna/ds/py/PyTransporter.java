package prerna.ds.py;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.AssetUtility;

public class PyTransporter {

	private static final Logger classLogger = LogManager.getLogger(PyTransporter.class);

	public static final String METHOD_DELIMITER = "$$##";
	public static String curEncoding = null;

	protected Logger logger = null;

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

	public PyTransporter() {
		this.logger = LogManager.getLogger(PyTransporter.class);
	}

	public SemossDataType convertDataType(String pDataType) {
		return pyS.get(pDataType);
	}
	
	/**
	 * 
	 * @param logger
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
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
	
	/**
	 * This becomes an issue on windows where it only consumes specific encoding
	 * @param insight
	 * @return
	 */
	public String getCurEncoding(Insight insight) {
		if (curEncoding == null) {
			curEncoding = (String) executePyDirect(insight, "sys.stdout.encoding");
		}
		return curEncoding;
	}

	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(Insight insight, String script) {
		return (List<Object>) transportScript(insight, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(Insight insight, String script) {
		List<String> val = (List<String>) transportScript(insight, script);
		return val;
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public String[] getStringArray(Insight insight, String script) {
		List<String> val = getStringList(insight, script);
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
	public boolean getBoolean(Insight insight, String script) {
		Boolean x = (Boolean) transportScript(insight, script);
		return x.booleanValue();
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(Insight insight, String script) {
		Number x = (Number) transportScript(insight, script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(Insight insight, String script) {
		Number x = (Number) transportScript(insight, script);
		return x.longValue();
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(Insight insight, String script) {
		Number x = (Number) transportScript(insight, script);
		return x.doubleValue();
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(Insight insight, String script) {
		return (String) transportScript(insight, script);
	}

	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param insight
	 * @param frameName
	 * @return
	 */
	public String[] getColumns(Insight insight, String frameName) {
		String script = "list(" + frameName + ".columns)";
		List<String> colNames = (List<String>) transportScript(insight, script);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	}
	
	/**
	 * 
	 * @param insight
	 * @param script
	 */
	public Object executePyDirect(Insight insight, String ... script) {
		String singleScript = convertArrayToString(script);
		return transportScript(insight, singleScript);
	}

	/**
	 * 
	 * @param insight
	 * @param inscript
	 * @return
	 */
	public Object executePyWithDefualtVars(Insight insight, String... script) {
		String[] paths = getDefaultPaths(insight);
		StringBuilder pathVars = generateDefaultVars(paths);
		transportScript(insight, pathVars.toString());

		String singleScript = convertArrayToString(script);
		Object output = transportScript(insight, singleScript);
		if(output instanceof String) {
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
		String[] pathVars = new String[] {"ROOT", "APP_ROOT", "USER_ROOT"};
		for(int i = 0; i < pathVars.length; i++) {
			if(defaultPaths[i] != null && !(defaultPaths[i]=defaultPaths[i].trim()).isEmpty()) {
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
			appPath = AssetUtility.getProjectAssetsFolder(insight.getContextProjectName(), insight.getContextProjectId());
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
		
		return new String[] {insightPath, appPath, userPath};
	}

	/**
	 * 
	 * @param script
	 * @return
	 */
	protected String convertArrayToString(String... script) {
		StringBuilder retString = new StringBuilder();
		for (int lineIndex = 0; lineIndex < script.length; lineIndex++) {
			if (script[lineIndex] != null) {
				retString.append(script[lineIndex]).append("\n");
			}
		}
		return retString.toString();
	}

	/**
	 * 
	 * @param insight
	 * @param script
	 * @return
	 */
	public Object transportScript(Insight insight, String script) {
		if(method != null) {
			script = method + METHOD_DELIMITER + script;
			method = null;
		}

		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = constructPayload(methodName, script);
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		// we always need an insight
		ps.insightId = insight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		
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
	 * @param methodName
	 * @param objects
	 * @return
	 */
	private PayloadStruct constructPayload(String methodName, Object...objects ) {
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.methodName = methodName;
		ps.payload = objects;
		return ps;
	}	

}
