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
	protected Logger logger = null;

	public static String curEncoding = null;

	private SocketClient sc = null;
	
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
			curEncoding = (String) transportScript(insight, null, "sys.stdout.encoding");
		}
		return curEncoding;
	}

	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Get list of Objects from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public List<Object> getList(Insight globalStoreInsight, String script) {
		return (List<Object>) transportScript(globalStoreInsight, null, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public List<String> getStringList(Insight globalStoreInsight, String script) {
		List<String> val = (List<String>) transportScript(globalStoreInsight, null, script);
		return val;
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public String[] getStringArray(Insight globalStoreInsight, String script) {
		List<String> val = getStringList(globalStoreInsight, script);
		String[] retString = new String[val.size()];
		val.toArray(retString);
		return retString;
	}

	/**
	 * Get boolean from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public boolean getBoolean(Insight globalStoreInsight, String script) {
		Boolean x = (Boolean) transportScript(globalStoreInsight, null, script);
		return x.booleanValue();
	}

	/**
	 * Get integer from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public int getInt(Insight globalStoreInsight, String script) {
		Number x = (Number) transportScript(globalStoreInsight, null, script);
		return x.intValue();
	}

	/**
	 * Get Long from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public Long getLong(Insight globalStoreInsight, String script) {
		Number x = (Number) transportScript(globalStoreInsight, null, script);
		return x.longValue();
	}

	/**
	 * Get double from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public double getDouble(Insight globalStoreInsight, String script) {
		Number x = (Number) transportScript(globalStoreInsight, null, script);
		return x.doubleValue();
	}

	/**
	 * Get String from py script
	 * 
	 * @param globalStoreInsight
	 * @param script
	 * @return
	 */
	public String getString(Insight globalStoreInsight, String script) {
		return (String) transportScript(globalStoreInsight, null, script);
	}

	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param globalStoreInsight
	 * @param frameName
	 * @return
	 */
	public String[] getColumns(Insight globalStoreInsight, String frameName) {
		String script = "list(" + frameName + ".columns)";
		List<String> colNames = (List<String>) transportScript(globalStoreInsight, null, script);
		String[] colNamesArray = new String[colNames.size()];
		colNamesArray = colNames.toArray(colNamesArray);
		return colNamesArray;
	}
	
	/**
	 * 
	 * @param globalStoreInsight
	 * @param inscript
	 * @return
	 */
	public Object executePyWithDefualtVars(Insight globalStoreInsight, String script) {
		return executePyWithDefualtVars(globalStoreInsight, null, script);
	}
	
	/**
	 * 
	 * @param globalStoreInsight
	 * @param executionInsight
	 * @param script
	 * @return
	 */
	public Object executePyWithDefualtVars(Insight globalStoreInsight, Insight executionInsight, String script) {
		String[] paths = getDefaultPaths(globalStoreInsight);
		StringBuilder pathVars = generateDefaultVars(paths);
		transportScript(globalStoreInsight, executionInsight, pathVars.toString());

		Object output = transportScript(globalStoreInsight, executionInsight, script);
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
	 * @param globalStoreInsight
	 * @param executionInsight
	 * @param script
	 * @return
	 */
	public Object transportScript(Insight globalStoreInsight, Insight executionInsight, String script) {
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.methodName = methodName;
		ps.payload = new Object[] {script};
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		// we always need an insight
		ps.insightId = globalStoreInsight.getInsightId();
		ps.jobId = ThreadStore.getJobId();
		if(executionInsight != null) {
        	ps.executionInsightId = executionInsight.getInsightId();
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
	 * @param globalStoreInsight
	 */
    public void clearInsightGlobals(Insight globalStoreInsight) {
        PayloadStruct ps = new PayloadStruct();
        ps.operation = PayloadStruct.OPERATION.INSIGHT;
        ps.payload = new Object[]{"CLEAR_NON_MODULE_GLOBALS"};
        ps.insightId = globalStoreInsight.getInsightId();
        if(sc.isConnected()) {
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			}
		} else {
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
    }

    /**
     * 
     * @param globalStoreInsight
     */
    public void removeInsightGlobals(Insight globalStoreInsight) {
        PayloadStruct ps = new PayloadStruct();
        ps.operation = PayloadStruct.OPERATION.INSIGHT;
        ps.payload = new Object[]{"REMOVE_INSIGHT_GLOBALS"};
        ps.insightId = globalStoreInsight.getInsightId();
        if(sc.isConnected()) {
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(ps != null && ps.ex != null) {
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			}
		} else {
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
    }
	
}
