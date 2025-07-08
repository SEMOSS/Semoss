package prerna.ds.py;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;

public class PyTranslator {

	protected PyTransporter pyTransporter = null;
	protected Insight insight = null;

	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	public Insight getInsight() {
		return this.insight;
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
		return this.pyTransporter.getCurEncoding(this.insight);
	}
	
	/**
	 * Get list of Objects from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<Object> getList(String script) {
		return this.pyTransporter.getList(this.insight, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public List<String> getStringList(String script) {
		return this.pyTransporter.getStringList(this.insight, script);
	}

	/**
	 * Get String[] from py script
	 * 
	 * @param script
	 * @return
	 */
	public String[] getStringArray(String script) {
		return this.pyTransporter.getStringArray(this.insight, script);
	}

	/**
	 * Get boolean from py script
	 * 
	 * @param script
	 * @return
	 */
	public boolean getBoolean(String script) {
		return this.pyTransporter.getBoolean(this.insight, script);
	}

	/**
	 * Get integer from py script
	 * 
	 * @param script
	 * @return
	 */
	public int getInt(String script) {
		return this.pyTransporter.getInt(this.insight, script);
	}

	/**
	 * Get Long from py script
	 * 
	 * @param script
	 * @return
	 */
	public Long getLong(String script) {
		return this.pyTransporter.getLong(this.insight, script);
	}

	/**
	 * Get double from py script
	 * 
	 * @param script
	 * @return
	 */
	public double getDouble(String script) {
		return this.pyTransporter.getDouble(this.insight, script);
	}

	/**
	 * Get String from py script
	 * 
	 * @param script
	 * @return
	 */
	public String getString(String script) {
		return this.pyTransporter.getString(this.insight, script);
	}
	
	/*
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.pyTransporter.getColumns(this.insight, frameName);
	}

	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the execution
	 * @param script
	 */
	public void runEmptyPy(String... script) {
		this.pyTransporter.executePyDirect(insight, script);
	}
	
	/**
	 * This does not append any variables (ROOT, APP_ROOT, USER_ROOT) with the execution
	 * @param script
	 */
	public Object runDirectPy(String... script) {
		if(script.length == 1) {
			return this.pyTransporter.transportScript(insight, script[0]);
		}
		
		return this.pyTransporter.executePyDirect(insight, script);
	}

	/**
	 * This will append ROOT, APP_ROOT, USER_ROOT variables to the execution
	 * @param script
	 * @return
	 */
	public Object runScript(String... script) {
		return this.pyTransporter.executePyWithDefualtVars(this.insight, script);
	}

	@Deprecated
	/**
	 * Switch to runDirectPy
	 * @param script
	 * @param insight
	 * @return
	 */
	public Object runSmssWrapperEval(String script) {
		return this.pyTransporter.transportScript(this.insight, script);
	}

}
