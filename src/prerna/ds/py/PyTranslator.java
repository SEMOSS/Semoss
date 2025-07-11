package prerna.ds.py;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;

public class PyTranslator {

	private PyTransporter pyTransporter = null;
	private Insight globalStoreInsight = null;

	public void setGlobalStoreInsight(Insight insight) {
		this.globalStoreInsight = insight;
	}
	
	public Insight getGlobalStoreInsight() {
		return this.globalStoreInsight;
	}
	
	public void setPyTransporter(PyTransporter pyTransporter) {
		this.pyTransporter = pyTransporter;
	}
	
	public PyTransporter getPyTransporter() {
		return this.pyTransporter;
	}
	
	public void setLogger(Logger logger) {
		this.pyTransporter.setLogger(logger);
	}

	public SemossDataType convertDataType(String pDataType) {
		return this.pyTransporter.convertDataType(pDataType);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getCurEncoding() {
		return this.pyTransporter.getCurEncoding(this.globalStoreInsight);
	}
	
	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(String script) {
		return this.pyTransporter.getList(this.globalStoreInsight, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(String script) {
		return this.pyTransporter.getStringList(this.globalStoreInsight, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public String[] getStringArray(String script) {
		return this.pyTransporter.getStringArray(this.globalStoreInsight, script);
	}

	/**
	 * Get boolean from py script
	 * 
	 * @param script
	 * @return
	 */
	public boolean getBoolean(String script) {
		return this.pyTransporter.getBoolean(this.globalStoreInsight, script);
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(String script) {
		return this.pyTransporter.getInt(this.globalStoreInsight, script);
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(String script) {
		return this.pyTransporter.getLong(this.globalStoreInsight, script);
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(String script) {
		return this.pyTransporter.getDouble(this.globalStoreInsight, script);
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(String script) {
		return this.pyTransporter.getString(this.globalStoreInsight, script);
	}
	
	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.pyTransporter.getColumns(this.globalStoreInsight, frameName);
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the execution
	 * @param script
	 */
	public void runEmptyPy(String... script) {
		this.pyTransporter.transportScript(globalStoreInsight, null, convertArrayToString(script));
	}
	
	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the execution
	 * @param script
	 */
	public Object runDirectPy(String... script) {
		return this.pyTransporter.transportScript(globalStoreInsight, null, convertArrayToString(script));
	}
	
	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the execution
	 * @param script
	 */
	public Object runDirectPy(Insight executionInsight, String... script) {
		return this.pyTransporter.transportScript(globalStoreInsight, executionInsight, convertArrayToString(script));
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * @param script
	 * @return
	 */
	public Object runScript(String... script) {
		return this.pyTransporter.executePyWithDefualtVars(this.globalStoreInsight, convertArrayToString(script));
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * @param script
	 * @return
	 */
	public Object runScript(Insight executionInsight, String... script) {
		return this.pyTransporter.executePyWithDefualtVars(this.globalStoreInsight, executionInsight, convertArrayToString(script));
	}
	
	@Deprecated
	/**
	 * Switch to runDirectPy
	 * @param script
	 * @param globalStoreInsight
	 * @return
	 */
	public Object runSmssWrapperEval(String script) {
		return this.pyTransporter.transportScript(this.globalStoreInsight, null, script);
	}
	
	@Deprecated
	/**
	 * Switch to runScript
	 * @param script
	 * @return
	 */
	public String runPyAndReturnOutput(String... script) {
		return this.pyTransporter.executePyWithDefualtVars(this.globalStoreInsight, convertArrayToString(script)) + "";
	}
	
	@Deprecated
	/**
	 * Switch to runScript
	 * @param script
	 * @return
	 */
	public String runSingle(String... script) {
		return this.pyTransporter.executePyWithDefualtVars(this.globalStoreInsight, convertArrayToString(script)) + "";
	}
	
	/**
	 * 
	 * @param globalStoreInsight
	 */
    public void clearInsightGlobals() {
    	this.pyTransporter.clearInsightGlobals(this.globalStoreInsight);
    }

    /**
     * 
     * @param globalStoreInsight
     */
    public void removeInsightGlobals() {
    	this.pyTransporter.removeInsightGlobals(this.globalStoreInsight);
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

}
